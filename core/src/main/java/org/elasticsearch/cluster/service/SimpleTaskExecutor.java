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
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.common.util.concurrent.PrioritizedRunnable;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public abstract class SimpleTaskExecutor<L> {

    protected final Logger logger;
    public final PrioritizedEsThreadPoolExecutor threadExecutor;
    protected final ThreadPool threadPool;

    protected SimpleTaskExecutor(Logger logger, PrioritizedEsThreadPoolExecutor threadExecutor, ThreadPool threadPool) {
        this.logger = logger;
        this.threadExecutor = threadExecutor;
        this.threadPool = threadPool;
    }

    @FunctionalInterface
    public interface RunTask<T, L> {
        void runTask(T task, L listener, String source);
    }

    /**
     * Submits a cluster state update task; submitted updates will be
     * batched across the same instance of executor. The exact batching
     * semantics depend on the underlying implementation but a rough
     * guideline is that if the update task is submitted while there
     * are pending update tasks for the same executor, these update
     * tasks will all be executed on the executor in a single batch
     *
     * @param source   the source of the cluster state update task
     * @param task     the state needed for the cluster state update task
     * @param config   the cluster state update task configuration
     * @param listener callback after the cluster state update task
     *                 completes
     * @param <T>      the type of the cluster state update task state
     *
     */
    public <T> void submitTask(final String source, T task, final ClusterStateTaskConfig config,
                               L listener, final RunTask<T, L> runTask) throws EsRejectedExecutionException {

        final UpdateTask<T, L> updateTask = new UpdateTask<>(source, task, config.priority(), listener,
            (UpdateTask<T, L> t) -> extractAndRun(t, runTask));

        final TimeValue timeout = config.timeout();
        if (timeout != null) {
            threadExecutor.execute(updateTask, threadPool.scheduler(), timeout, () -> onTimeout(updateTask, source, timeout));
        } else {
            threadExecutor.execute(updateTask);
        }
    }

    public <T> void extractAndRun(UpdateTask<T, L> updateTask, RunTask<T, L> runTask) {
        if (updateTask.processed.getAndSet(true) == false) {
            logger.trace("will process {}", updateTask.task);
            runTask.runTask(updateTask.task, updateTask.listener, updateTask.source);
        } else {
            logger.trace("skipping {}, already processed", updateTask.task);
        }
    }

    private <T> void onTimeout(UpdateTask<T, L> updateTask, String source, TimeValue timeout) {
        threadPool.generic().execute(() -> {
            if (updateTask.processed.getAndSet(true) == false) {
                logger.debug("cluster state update task [{}] timed out after [{}]", source, timeout);
                onTimeout(updateTask.source, updateTask.listener, timeout);
            }
        });
    }

    protected abstract void onTimeout(String source, L listener, TimeValue timeout);

    /**
     * Returns the tasks that are pending.
     */
    public List<PendingClusterTask> pendingTasks() {
        PrioritizedEsThreadPoolExecutor.Pending[] pendings = threadExecutor.getPending();
        List<PendingClusterTask> pendingClusterTasks = new ArrayList<>(pendings.length);
        for (PrioritizedEsThreadPoolExecutor.Pending pending : pendings) {
            final String source;
            final long timeInQueue;
            // we have to capture the task as it will be nulled after execution and we don't want to change while we check things here.
            final Object task = pending.task;
            if (task == null) {
                continue;
            } else if (task instanceof SourcePrioritizedRunnable) {
                SourcePrioritizedRunnable runnable = (SourcePrioritizedRunnable) task;
                source = runnable.source();
                timeInQueue = runnable.getAgeInMillis();
            } else {
                assert false : "expected SourcePrioritizedRunnable got " + task.getClass();
                source = "unknown [" + task.getClass() + "]";
                timeInQueue = 0;
            }

            pendingClusterTasks.add(
                new PendingClusterTask(pending.insertionOrder, pending.priority, new Text(source), timeInQueue, pending.executing));
        }
        return pendingClusterTasks;
    }

    /**
     * Returns the number of currently pending tasks.
     */
    public int numberOfPendingTasks() {
        return threadExecutor.getNumberOfPendingTasks();
    }

    /**
     * Returns the maximum wait time for tasks in the queue
     *
     * @return A zero time value if the queue is empty, otherwise the time value oldest task waiting in the queue
     */
    public TimeValue getMaxTaskWaitTime() {
        return threadExecutor.getMaxTaskWaitTime();
    }

    protected abstract static class SourcePrioritizedRunnable extends PrioritizedRunnable {
        protected final String source;

        public SourcePrioritizedRunnable(Priority priority, String source) {
            super(priority);
            this.source = source;
        }

        public String source() {
            return source;
        }
    }

    protected static class UpdateTask<T, L> extends SourcePrioritizedRunnable {

        public final T task;
        public final L listener;
        public final AtomicBoolean processed = new AtomicBoolean();
        private final Consumer<UpdateTask<T, L>> runnable;

        UpdateTask(String source, T task, Priority priority, L listener, Consumer<UpdateTask<T, L>> runnable) {
            super(priority, source);
            this.task = task;
            this.listener = listener;
            this.runnable = runnable;
        }

        @Override
        public void run() {
            runnable.accept(this);
        }

        @Override
        public String toString() {
            return "[" + source + "]";
        }
    }
}
