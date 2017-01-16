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

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskExecutor.ClusterTasksResult;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.metadata.ProcessClusterEventTimeoutException;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.common.util.concurrent.PrioritizedRunnable;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public abstract class AbstractClusterTaskExecutor extends AbstractLifecycleComponent {

    protected final ThreadPool threadPool;
    protected final ClusterSettings clusterSettings;
    final Map<ClusterStateTaskExecutor, LinkedHashSet<UpdateTask>> updateTasksPerExecutor = new HashMap<>();
    protected final ClusterName clusterName;
    protected volatile PrioritizedEsThreadPoolExecutor threadExecutor;

    protected AbstractClusterTaskExecutor(Settings settings, ClusterSettings clusterSettings, ThreadPool threadPool) {
        super(settings);
        this.threadPool = threadPool;
        this.clusterSettings = clusterSettings;
        this.clusterName = ClusterName.CLUSTER_NAME_SETTING.get(settings);
    }

    @Override
    protected void doStart() {

    }

    @Override
    protected void doStop() {

    }

    public ClusterName getClusterName() {
        return clusterName;
    }

    public ClusterSettings getClusterSettings() {
        return clusterSettings;
    }

    public Settings getSettings() {
        return settings;
    }

    /**
     * Submits a cluster state update task; unlike {@link #submitStateUpdateTask(String, Object, ClusterStateTaskConfig,
     * ClusterStateTaskExecutor, ClusterStateTaskListener)}, submitted updates will not be batched.
     *
     * @param source     the source of the cluster state update task
     * @param updateTask the full context for the cluster state update
     *                   task
     *
     */
    public <T extends ClusterStateTaskConfig & ClusterStateTaskExecutor<T> & ClusterStateTaskListener> void submitStateUpdateTask(
        final String source, final T updateTask) {
        submitStateUpdateTask(source, updateTask, updateTask, updateTask, updateTask);
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
     * @param executor the cluster state update task executor; tasks
     *                 that share the same executor will be executed
     *                 batches on this executor
     * @param listener callback after the cluster state update task
     *                 completes
     * @param <T>      the type of the cluster state update task state
     *
     */
    public <T> void submitStateUpdateTask(final String source, final T task,
                                          final ClusterStateTaskConfig config,
                                          final ClusterStateTaskExecutor<T> executor,
                                          final ClusterStateTaskListener listener) {
        submitStateUpdateTasks(source, Collections.singletonMap(task, listener), config, executor);
    }

    /**
     * Submits a batch of cluster state update tasks; submitted updates are guaranteed to be processed together,
     * potentially with more tasks of the same executor.
     *
     * @param source   the source of the cluster state update task
     * @param tasks    a map of update tasks and their corresponding listeners
     * @param config   the cluster state update task configuration
     * @param executor the cluster state update task executor; tasks
     *                 that share the same executor will be executed
     *                 batches on this executor
     * @param <T>      the type of the cluster state update task state
     *
     */
    public <T> void submitStateUpdateTasks(final String source,
                                           final Map<T, ClusterStateTaskListener> tasks, final ClusterStateTaskConfig config,
                                           final ClusterStateTaskExecutor<T> executor) {
        if (!lifecycle.started()) {
            return;
        }
        if (tasks.isEmpty()) {
            return;
        }
        try {
            @SuppressWarnings("unchecked")
            ClusterStateTaskExecutor<Object> taskExecutor = (ClusterStateTaskExecutor<Object>) executor;
            // convert to an identity map to check for dups based on update tasks semantics of using identity instead of equal
            final IdentityHashMap<Object, ClusterStateTaskListener> tasksIdentity = new IdentityHashMap<>(tasks);
            final List<UpdateTask> updateTasks = tasksIdentity.entrySet().stream().map(
                entry -> new UpdateTask(source, entry.getKey(), config.priority(), taskExecutor, safe(entry.getValue()))
            ).collect(Collectors.toList());

            synchronized (updateTasksPerExecutor) {
                LinkedHashSet<UpdateTask> existingTasks = updateTasksPerExecutor.computeIfAbsent(executor,
                    k -> new LinkedHashSet<>(updateTasks.size()));
                for (UpdateTask existing : existingTasks) {
                    if (tasksIdentity.containsKey(existing.task)) {
                        throw new IllegalStateException("task [" + taskExecutor.describeTasks(Collections.singletonList(existing.task)) +
                            "] with source [" + source + "] is already queued");
                    }
                }
                existingTasks.addAll(updateTasks);
            }

            final UpdateTask firstTask = updateTasks.get(0);

            final TimeValue timeout = config.timeout();
            if (timeout != null) {
                threadExecutor.execute(firstTask, threadPool.scheduler(), timeout, () -> onTimeout(updateTasks, source, timeout));
            } else {
                threadExecutor.execute(firstTask);
            }
        } catch (EsRejectedExecutionException e) {
            // ignore cases where we are shutting down..., there is really nothing interesting
            // to be done here...
            if (!lifecycle.stoppedOrClosed()) {
                throw e;
            }
        }
    }

    private void onTimeout(List<UpdateTask> updateTasks, String source, TimeValue timeout) {
        threadPool.generic().execute(() -> {
            final ArrayList<UpdateTask> toRemove = new ArrayList<>();
            for (UpdateTask task : updateTasks) {
                if (task.processed.getAndSet(true) == false) {
                    logger.debug("cluster state update task [{}] timed out after [{}]", source, timeout);
                    toRemove.add(task);
                }
            }
            if (toRemove.isEmpty() == false) {
                ClusterStateTaskExecutor<Object> clusterStateTaskExecutor = toRemove.get(0).executor;
                synchronized (updateTasksPerExecutor) {
                    LinkedHashSet<UpdateTask> existingTasks = updateTasksPerExecutor.get(clusterStateTaskExecutor);
                    if (existingTasks != null) {
                        existingTasks.removeAll(toRemove);
                        if (existingTasks.isEmpty()) {
                            updateTasksPerExecutor.remove(clusterStateTaskExecutor);
                        }
                    }
                }
                for (UpdateTask task : toRemove) {
                    task.listener.onFailure(source, new ProcessClusterEventTimeoutException(timeout, source));
                }
            }
        });
    }

    protected abstract ClusterStateTaskListener safe(ClusterStateTaskListener value);

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
            } else if (task instanceof ClusterApplierService.SourcePrioritizedRunnable) {
                ClusterApplierService.SourcePrioritizedRunnable runnable = (ClusterApplierService.SourcePrioritizedRunnable) task;
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

    protected class UpdateTask extends SourcePrioritizedRunnable {

        public final Object task;
        public final ClusterStateTaskListener listener;
        protected final ClusterStateTaskExecutor<Object> executor;
        public final AtomicBoolean processed = new AtomicBoolean();

        UpdateTask(String source, Object task, Priority priority, ClusterStateTaskExecutor<Object> executor,
                   ClusterStateTaskListener listener) {
            super(priority, source);
            this.task = task;
            this.executor = executor;
            this.listener = listener;
        }

        @Override
        public void run() {
            // if this task is already processed, the executor shouldn't execute other tasks (that arrived later),
            // to give other executors a chance to execute their tasks.
            if (processed.get() == false) {
                final ArrayList<UpdateTask> toExecute = new ArrayList<>();
                final Map<String, ArrayList<Object>> processTasksBySource = new HashMap<>();
                synchronized (updateTasksPerExecutor) {
                    LinkedHashSet<UpdateTask> pending = updateTasksPerExecutor.remove(executor);
                    if (pending != null) {
                        for (UpdateTask task : pending) {
                            if (task.processed.getAndSet(true) == false) {
                                logger.trace("will process {}", task);
                                toExecute.add(task);
                                processTasksBySource.computeIfAbsent(task.source, s -> new ArrayList<>()).add(task.task);
                            } else {
                                logger.trace("skipping {}, already processed", task);
                            }
                        }
                    }
                }

                if (toExecute.isEmpty() == false) {
                    final String tasksSummary = processTasksBySource.entrySet().stream().map(entry -> {
                        String tasks = executor.describeTasks(entry.getValue());
                        return tasks.isEmpty() ? entry.getKey() : entry.getKey() + "[" + tasks + "]";
                    }).reduce((s1, s2) -> s1 + ", " + s2).orElse("");

                    runTasks(new TaskInputs(executor, toExecute, tasksSummary));
                }
            }
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

    abstract static class SourcePrioritizedRunnable extends PrioritizedRunnable {
        protected final String source;

        public SourcePrioritizedRunnable(Priority priority, String source) {
            super(priority);
            this.source = source;
        }

        public String source() {
            return source;
        }
    }

    // this one is overridden in tests so we can control time
    protected long currentTimeInNanos() {
        return System.nanoTime();
    }


    protected abstract void runTasks(TaskInputs taskInputs);

    protected ClusterTasksResult<Object> executeTasks(TaskInputs taskInputs, long startTimeNS, ClusterState previousClusterState) {
        ClusterTasksResult<Object> clusterTasksResult;
        try {
            List<Object> inputs = taskInputs.updateTasks.stream().map(tUpdateTask -> tUpdateTask.task).collect(Collectors.toList());
            clusterTasksResult = taskInputs.executor.execute(previousClusterState, inputs);
        } catch (Exception e) {
            TimeValue executionTime = TimeValue.timeValueMillis(Math.max(0, TimeValue.nsecToMSec(currentTimeInNanos() - startTimeNS)));
            if (logger.isTraceEnabled()) {
                logger.trace(
                    (Supplier<?>) () -> new ParameterizedMessage(
                        "failed to execute cluster state update in [{}], state:\nversion [{}], source [{}]\n{}{}{}",
                        executionTime,
                        previousClusterState.version(),
                        taskInputs.summary,
                        previousClusterState.nodes(),
                        previousClusterState.routingTable(),
                        previousClusterState.getRoutingNodes()),
                    e);
            }
            warnAboutSlowTaskIfNeeded(executionTime, taskInputs.summary);
            clusterTasksResult = ClusterTasksResult.builder()
                .failures(taskInputs.updateTasks.stream().map(updateTask -> updateTask.task)::iterator, e)
                .build(previousClusterState);
        }

        assert clusterTasksResult.executionResults != null;
        assert clusterTasksResult.executionResults.size() == taskInputs.updateTasks.size()
            : String.format(Locale.ROOT, "expected [%d] task result%s but was [%d]", taskInputs.updateTasks.size(),
            taskInputs.updateTasks.size() == 1 ? "" : "s", clusterTasksResult.executionResults.size());
        boolean assertsEnabled = false;
        assert (assertsEnabled = true);
        if (assertsEnabled) {
            for (UpdateTask updateTask : taskInputs.updateTasks) {
                assert clusterTasksResult.executionResults.containsKey(updateTask.task) :
                    "missing task result for " + updateTask;
            }
        }

        return clusterTasksResult;
    }

    public List<UpdateTask> getNonFailedTasks(TaskInputs taskInputs, ClusterTasksResult<Object> clusterTasksResult) {
        List<UpdateTask> nonFailedTasks = new ArrayList<>();
        for (UpdateTask updateTask : taskInputs.updateTasks) {
            assert clusterTasksResult.executionResults.containsKey(updateTask.task) : "missing " + updateTask;
            final ClusterStateTaskExecutor.TaskResult taskResult =
                clusterTasksResult.executionResults.get(updateTask.task);
            if (taskResult.isSuccess()) {
                nonFailedTasks.add(updateTask);
            }
        }
        return nonFailedTasks;
    }

    /**
     * Represents a set of tasks to be processed together with their executor
     */
    protected class TaskInputs {
        public final String summary;
        public final ArrayList<UpdateTask> updateTasks;
        public final ClusterStateTaskExecutor<Object> executor;

        TaskInputs(ClusterStateTaskExecutor<Object> executor, ArrayList<UpdateTask> updateTasks, String summary) {
            this.summary = summary;
            this.executor = executor;
            this.updateTasks = updateTasks;
        }

        public boolean runOnlyOnMaster() {
            return executor.runOnlyOnMaster();
        }

        public boolean isPublishingTask() {
            return executor.isDiscoveryServiceTask();
        }

        public void onNoLongerMaster() {
            updateTasks.stream().forEach(task -> task.listener.onNoLongerMaster(task.source()));
        }
    }

    protected abstract void warnAboutSlowTaskIfNeeded(TimeValue executionTime, String summary);
}
