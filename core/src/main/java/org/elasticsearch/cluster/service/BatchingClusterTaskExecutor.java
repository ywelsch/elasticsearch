/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.service;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public abstract class BatchingClusterTaskExecutor<L> extends SimpleTaskExecutor<L> {

    public interface BatchingExecutor<T> {
        /**
         * Builds a concise description of a list of tasks (to be used in logging etc.).
         *
         * This method can be called multiple times with different lists before execution.
         * This allows groupd task description but the submitting source.
         */
        default String describeTasks(List<T> tasks) {
            return tasks.stream().map(T::toString).reduce((s1,s2) -> {
                if (s1.isEmpty()) {
                    return s2;
                } else if (s2.isEmpty()) {
                    return s1;
                } else {
                    return s1 + ", " + s2;
                }
            }).orElse("");
        }
    }

    @FunctionalInterface
    public interface RunTasks<T, L, Q extends BatchingExecutor<T>> {
        void runTasks(Q executor, List<BatchingUpdateTask<T, L, Q>> toExecute, String tasksSummary);
    }

    final Map<BatchingExecutor<Object>, LinkedHashSet<BatchingUpdateTask>> updateTasksPerExecutor = new HashMap<>();

    protected BatchingClusterTaskExecutor(Logger logger, PrioritizedEsThreadPoolExecutor threadExecutor, ThreadPool threadPool) {
        super(logger, threadExecutor, threadPool);
    }

    public <T, B extends BatchingExecutor<T>> void submitTask(final String source, T task, final ClusterStateTaskConfig config,
                           final B executor, L listener, final RunTasks<T, L, B> runTasks) throws EsRejectedExecutionException {
        submitTasks(source, Collections.singletonMap(task, listener), config, executor, runTasks);
    }

    public <T, B extends BatchingExecutor<T>> void submitTasks(final String source, final Map<T, L> tasks,
                                                               final ClusterStateTaskConfig config, final B executor,
                                                               final RunTasks<T, L, B> runTasks) throws EsRejectedExecutionException {
        if (tasks.isEmpty()) {
            return;
        }
        @SuppressWarnings("unchecked")
        BatchingExecutor<Object> batchingExecutor = (BatchingExecutor<Object>) executor;
        // convert to an identity map to check for dups based on update tasks semantics of using identity instead of equal
        final IdentityHashMap<T, L> tasksIdentity = new IdentityHashMap<>(tasks);
        final List<BatchingUpdateTask> updateTasks = tasksIdentity.entrySet().stream().map(
            entry -> new BatchingUpdateTask<>(source, entry.getKey(), entry.getValue(), config.priority(), executor,
                (BatchingUpdateTask<T, L, B> updateTask) -> extractAndRun(updateTask, runTasks))
        ).collect(Collectors.toList());

        synchronized (updateTasksPerExecutor) {
            LinkedHashSet<BatchingUpdateTask> existingTasks = updateTasksPerExecutor.computeIfAbsent(batchingExecutor,
                k -> new LinkedHashSet<>(updateTasks.size()));
            for (BatchingUpdateTask existing : existingTasks) {
                if (tasksIdentity.containsKey(existing.task)) {
                    throw new IllegalStateException("task [" + batchingExecutor.describeTasks(Collections.singletonList(existing.task)) +
                        "] with source [" + source + "] is already queued");
                }
            }
            existingTasks.addAll(updateTasks);
        }

        final BatchingUpdateTask firstTask = updateTasks.get(0);

        final TimeValue timeout = config.timeout();
        if (timeout != null) {
            threadExecutor.execute(firstTask, threadPool.scheduler(), timeout, () -> onTimeout(updateTasks, source, timeout));
        } else {
            threadExecutor.execute(firstTask);
        }
    }

    private void onTimeout(List<BatchingUpdateTask> updateTasks, String source, TimeValue timeout) {
        threadPool.generic().execute(() -> {
            final ArrayList<BatchingUpdateTask> toRemove = new ArrayList<>();
            for (BatchingUpdateTask task : updateTasks) {
                if (task.processed.getAndSet(true) == false) {
                    logger.debug("cluster state update task [{}] timed out after [{}]", source, timeout);
                    toRemove.add(task);
                }
            }
            if (toRemove.isEmpty() == false) {
                BatchingExecutor<Object> batchingExecutor = toRemove.get(0).executor;
                synchronized (updateTasksPerExecutor) {
                    LinkedHashSet<BatchingUpdateTask> existingTasks = updateTasksPerExecutor.get(batchingExecutor);
                    if (existingTasks != null) {
                        existingTasks.removeAll(toRemove);
                        if (existingTasks.isEmpty()) {
                            updateTasksPerExecutor.remove(batchingExecutor);
                        }
                    }
                }
                for (BatchingUpdateTask task : toRemove) {
                    onTimeout(task.source, (L)task.listener, timeout);
                }
            }
        });
    }

    public <T, Q extends BatchingExecutor<T>> void extractAndRun(BatchingUpdateTask<T, L, Q> updateTask, RunTasks<T, L, Q> runTasks) {
        // if this task is already processed, the executor shouldn't execute other tasks (that arrived later),
        // to give other executors a chance to execute their tasks.
        if (updateTask.processed.get() == false) {
            final List<BatchingUpdateTask<T, L, Q>> toExecute = new ArrayList<>();
            final Map<String, List<T>> processTasksBySource = new HashMap<>();
            synchronized (updateTasksPerExecutor) {
                LinkedHashSet<BatchingUpdateTask> pending = updateTasksPerExecutor.remove(updateTask.executor);
                if (pending != null) {
                    for (BatchingUpdateTask task : pending) {
                        if (task.processed.getAndSet(true) == false) {
                            logger.trace("will process {}", task);
                            toExecute.add(task);
                            processTasksBySource.computeIfAbsent(task.source, s -> new ArrayList<>()).add((T) task.task);
                        } else {
                            logger.trace("skipping {}, already processed", task);
                        }
                    }
                }
            }

            if (toExecute.isEmpty() == false) {
                final String tasksSummary = processTasksBySource.entrySet().stream().map(entry -> {
                    String tasks = updateTask.executor.describeTasks(entry.getValue());
                    return tasks.isEmpty() ? entry.getKey() : entry.getKey() + "[" + tasks + "]";
                }).reduce((s1, s2) -> s1 + ", " + s2).orElse("");

                runTasks.runTasks(updateTask.executor, toExecute, tasksSummary);
            }
        }
    }

    public static class BatchingUpdateTask<T, L, Q extends BatchingExecutor<T>> extends SourcePrioritizedRunnable {

        public final T task;
        public final L listener;
        private final Consumer<BatchingUpdateTask<T, L, Q>> runnable;
        final Q executor;
        final AtomicBoolean processed = new AtomicBoolean();

        BatchingUpdateTask(String source, T task, L listener, Priority priority, Q executor,
                           Consumer<BatchingUpdateTask<T, L, Q>> runnable) {
            super(priority, source);
            this.task = task;
            this.executor = executor;
            this.listener = listener;
            this.runnable = runnable;
        }

        @Override
        public final void run() {
            runnable.accept(this);
        }

        @Override
        public String toString() {
            String taskDescription = executor.describeTasks(Collections.singletonList(task));
            if (taskDescription.isEmpty()) {
                return "[" + source + "]";
            } else {
                return "[" + source + "[" + taskDescription + "]]";
            }
        }
    }
}
