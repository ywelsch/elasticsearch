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
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.cluster.AckedClusterStateTaskListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterState.Builder;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskExecutor.ClusterTasksResult;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.PublishingClusterStateTaskListener;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.ProcessClusterEventTimeoutException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.service.BatchingClusterTaskExecutor.BatchingUpdateTask;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.MasterServiceListener;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.service.ClusterService.CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING;
import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;

public class MasterService extends AbstractLifecycleComponent implements RunOnMaster {

    public static final String MASTER_UPDATE_THREAD_NAME = "masterService#updateTask";

    private BiConsumer<ClusterChangedEvent, Discovery.AckListener> clusterStatePublisher;

    private volatile TimeValue slowTaskLoggingThreshold;

    private final Collection<MasterServiceListener> stateListeners = new CopyOnWriteArrayList<>();

    private java.util.function.Supplier<ClusterState> clusterStateSupplier;

    protected final ThreadPool threadPool;

    protected volatile BatchingClusterTaskExecutor<PublishingClusterStateTaskListener> batchingClusterTaskExecutor;

    public MasterService(Settings settings, ThreadPool threadPool) {
        super(settings);
        // TODO: introduce a dedicated setting for master service
        this.slowTaskLoggingThreshold = CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING.get(settings);
        this.threadPool = threadPool;
    }

    public void setSlowTaskLoggingThreshold(TimeValue slowTaskLoggingThreshold) {
        this.slowTaskLoggingThreshold = slowTaskLoggingThreshold;
    }

    public synchronized void setClusterStatePublisher(BiConsumer<ClusterChangedEvent, Discovery.AckListener> publisher) {
        clusterStatePublisher = publisher;
    }

    public synchronized void setClusterStateSupplier(java.util.function.Supplier<ClusterState> clusterStateSupplier) {
        this.clusterStateSupplier = clusterStateSupplier;
    }

    @Override
    protected synchronized void doStart() {
        Objects.requireNonNull(clusterStatePublisher, "please set a cluster state publisher before starting");
        Objects.requireNonNull(clusterStateSupplier, "please set a cluster state supplier before starting");
        PrioritizedEsThreadPoolExecutor threadExecutor = EsExecutors.newSinglePrioritizing(MASTER_UPDATE_THREAD_NAME,
            daemonThreadFactory(settings, MASTER_UPDATE_THREAD_NAME), threadPool.getThreadContext());
        batchingClusterTaskExecutor = new BatchingClusterTaskExecutor<PublishingClusterStateTaskListener>(logger, threadExecutor, threadPool) {
            @Override
            protected void onTimeout(String source, PublishingClusterStateTaskListener listener, TimeValue timeout) {
                listener.onFailure(source, new ProcessClusterEventTimeoutException(timeout, source));
            }
        };
    }

    @Override
    protected synchronized void doStop() {
        ThreadPool.terminate(batchingClusterTaskExecutor.threadExecutor, 10, TimeUnit.SECONDS);
    }

    @Override
    protected synchronized void doClose() {
    }

    /**
     * The current cluster state.
     */
    public ClusterState state() {
        return this.clusterStateSupplier.get();
    }

    public static boolean assertMasterUpdateThread() {
        assert Thread.currentThread().getName().contains(MASTER_UPDATE_THREAD_NAME) :
            "not called from the master service thread";
        return true;
    }

    public static boolean assertNotMasterUpdateThread(String reason) {
        assert Thread.currentThread().getName().contains(MASTER_UPDATE_THREAD_NAME) == false :
            "Expected current thread [" + Thread.currentThread() + "] to not be the master service thread. Reason: [" + reason + "]";
        return true;
    }

    protected <T> void runTasks(TaskInputs<T> taskInputs) {
        final String summary = taskInputs.summary;
        if (!lifecycle.started()) {
            logger.debug("processing [{}]: ignoring, cluster service not started", summary);
            return;
        }

        logger.debug("processing [{}]: execute", summary);
        final ClusterState previousClusterState = state();

        if (!previousClusterState.nodes().isLocalNodeElectedMaster() && taskInputs.runOnlyOnMaster()) {
            logger.debug("failing [{}]: local node is no longer master", summary);
            taskInputs.onNoLongerMaster();
            return;
        }

        long startTimeNS = currentTimeInNanos();
        TaskOutputs taskOutputs = calculateTaskOutputs(taskInputs, previousClusterState, startTimeNS);
        taskOutputs.notifyFailedTasks();

        if (taskOutputs.clusterStateUnchanged()) {
            taskOutputs.notifySuccessfulTasksOnUnchangedClusterState();
            TimeValue executionTime = TimeValue.timeValueMillis(Math.max(0, TimeValue.nsecToMSec(currentTimeInNanos() - startTimeNS)));
            logger.debug("processing [{}]: took [{}] no change in cluster_state", summary, executionTime);
            warnAboutSlowTaskIfNeeded(executionTime, summary);
        } else {
            ClusterState newClusterState = taskOutputs.newClusterState;
            if (logger.isTraceEnabled()) {
                logger.trace("cluster state updated, source [{}]\n{}", summary, newClusterState);
            } else if (logger.isDebugEnabled()) {
                logger.debug("cluster state updated, version [{}], source [{}]", newClusterState.version(), summary);
            }
            try {
                ClusterChangedEvent clusterChangedEvent = new ClusterChangedEvent(summary, newClusterState, previousClusterState);
                // new cluster state, notify all listeners
                final DiscoveryNodes.Delta nodesDelta = clusterChangedEvent.nodesDelta();
                if (nodesDelta.hasChanges() && logger.isInfoEnabled()) {
                    String nodeSummary = nodesDelta.shortSummary();
                    if (nodeSummary.length() > 0) {
                        logger.info("{}, reason: {}", summary, nodeSummary);
                    }
                }

                logger.debug("publishing cluster state version [{}]", newClusterState.version());
                try {
                    clusterStatePublisher.accept(clusterChangedEvent, taskOutputs.createAckListener(threadPool, newClusterState));
                } catch (Discovery.FailedToCommitClusterStateException t) {
                    final long version = newClusterState.version();
                    logger.warn(
                        (Supplier<?>) () -> new ParameterizedMessage(
                            "failing [{}]: failed to commit cluster state version [{}]", summary, version),
                        t);
                    taskOutputs.publishingFailed(t);
                    return;
                } catch (NotMasterException e) {
                    logger.debug("failing [{}]: local node is no longer master", summary);
                    taskInputs.onNoLongerMaster();
                    return;
                }

                stateListeners.forEach(listener -> {
                    try {
                        logger.trace("calling [{}] with change to version [{}]", listener, newClusterState.version());
                        listener.clusterChanged(previousClusterState, newClusterState);
                    } catch (Exception ex) {
                        logger.warn("failed to notify MasterServiceListener", ex);
                    }
                });

                taskOutputs.processedDifferentClusterState(previousClusterState, newClusterState);

                try {
                    taskOutputs.clusterStatePublished(clusterChangedEvent);
                } catch (Exception e) {
                    logger.error(
                        (Supplier<?>) () -> new ParameterizedMessage(
                            "exception thrown while notifying executor of new cluster state publication [{}]",
                            summary),
                        e);
                }
                TimeValue executionTime = TimeValue.timeValueMillis(Math.max(0, TimeValue.nsecToMSec(currentTimeInNanos() - startTimeNS)));
                logger.debug("processing [{}]: took [{}] done publishing updated cluster_state (version: {}, uuid: {})", summary,
                    executionTime, newClusterState.version(),
                    newClusterState.stateUUID());
                warnAboutSlowTaskIfNeeded(executionTime, summary);
            } catch (Exception e) {
                TimeValue executionTime = TimeValue.timeValueMillis(Math.max(0, TimeValue.nsecToMSec(currentTimeInNanos() - startTimeNS)));
                final long version = newClusterState.version();
                final String stateUUID = newClusterState.stateUUID();
                final String fullState = newClusterState.toString();
                logger.warn(
                    (Supplier<?>) () -> new ParameterizedMessage(
                        "failed to apply updated cluster state in [{}]:\nversion [{}], uuid [{}], source [{}]\n{}",
                        executionTime,
                        version,
                        stateUUID,
                        summary,
                        fullState),
                    e);
                // TODO: do we want to call updateTask.onFailure here?
            }
        }
    }

    public <T> TaskOutputs<T> calculateTaskOutputs(TaskInputs<T> taskInputs, ClusterState previousClusterState, long startTimeNS) {
        ClusterTasksResult<T> clusterTasksResult = executeTasks(taskInputs, startTimeNS, previousClusterState);
        ClusterState newClusterState = patchVersions(previousClusterState, clusterTasksResult);
        return new TaskOutputs(taskInputs, previousClusterState, newClusterState, getNonFailedTasks(taskInputs, clusterTasksResult),
            clusterTasksResult.executionResults);
    }

    private ClusterState patchVersions(ClusterState previousClusterState, ClusterTasksResult<?> executionResult) {
        ClusterState newClusterState = executionResult.resultingState;

        if (previousClusterState != newClusterState) {
            // only the master controls the version numbers
            Builder builder = ClusterState.builder(newClusterState).incrementVersion();
            if (previousClusterState.routingTable() != newClusterState.routingTable()) {
                builder.routingTable(RoutingTable.builder(newClusterState.routingTable())
                    .version(newClusterState.routingTable().version() + 1).build());
            }
            if (previousClusterState.metaData() != newClusterState.metaData()) {
                builder.metaData(MetaData.builder(newClusterState.metaData()).version(newClusterState.metaData().version() + 1));
            }

            newClusterState = builder.build();
        }

        return newClusterState;
    }

    /**
     * Output created by executing a set of tasks provided as TaskInputs
     */
    class TaskOutputs<T> {
        public final TaskInputs<T> taskInputs;
        public final ClusterState previousClusterState;
        public final ClusterState newClusterState;
        public final List<BatchingUpdateTask<T, PublishingClusterStateTaskListener, ClusterStateTaskExecutor<T>>> nonFailedTasks;
        public final Map<Object, ClusterStateTaskExecutor.TaskResult> executionResults;

        TaskOutputs(TaskInputs taskInputs, ClusterState previousClusterState,
                           ClusterState newClusterState,
                           List<BatchingUpdateTask<T, PublishingClusterStateTaskListener, ClusterStateTaskExecutor<T>>> nonFailedTasks,
                           Map<Object, ClusterStateTaskExecutor.TaskResult> executionResults) {
            this.taskInputs = taskInputs;
            this.previousClusterState = previousClusterState;
            this.newClusterState = newClusterState;
            this.nonFailedTasks = nonFailedTasks;
            this.executionResults = executionResults;
        }

        public void publishingFailed(Discovery.FailedToCommitClusterStateException t) {
            nonFailedTasks.forEach(task -> task.listener.onFailure(task.source(), t));
        }

        public void processedDifferentClusterState(ClusterState previousClusterState, ClusterState newClusterState) {
            nonFailedTasks.forEach(task -> task.listener.clusterStateProcessed(task.source(), previousClusterState, newClusterState));
        }

        public void clusterStatePublished(ClusterChangedEvent clusterChangedEvent) {
            taskInputs.executor.clusterStatePublished(clusterChangedEvent);
        }

        public Discovery.AckListener createAckListener(ThreadPool threadPool, ClusterState newClusterState) {
            ArrayList<Discovery.AckListener> ackListeners = new ArrayList<>();

            //timeout straightaway, otherwise we could wait forever as the timeout thread has not started
            nonFailedTasks.stream().filter(task -> task.listener instanceof AckedClusterStateTaskListener).forEach(task -> {
                final AckedClusterStateTaskListener ackedListener = (AckedClusterStateTaskListener) task.listener;
                if (ackedListener.ackTimeout() == null || ackedListener.ackTimeout().millis() == 0) {
                    ackedListener.onAckTimeout();
                } else {
                    try {
                        ackListeners.add(new AckCountDownListener(ackedListener, newClusterState.version(), newClusterState.nodes(),
                            threadPool));
                    } catch (EsRejectedExecutionException ex) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Couldn't schedule timeout thread - node might be shutting down", ex);
                        }
                        //timeout straightaway, otherwise we could wait forever as the timeout thread has not started
                        ackedListener.onAckTimeout();
                    }
                }
            });

            return new DelegetingAckListener(ackListeners);
        }

        public boolean clusterStateUnchanged() {
            return previousClusterState == newClusterState;
        }

        public void notifyFailedTasks() {
            // fail all tasks that have failed
            for (BatchingUpdateTask<T, PublishingClusterStateTaskListener, ClusterStateTaskExecutor<T>> updateTask : taskInputs.updateTasks) {
                assert executionResults.containsKey(updateTask.task) : "missing " + updateTask;
                final ClusterStateTaskExecutor.TaskResult taskResult = executionResults.get(updateTask.task);
                if (taskResult.isSuccess() == false) {
                    updateTask.listener.onFailure(updateTask.source(), taskResult.getFailure());
                }
            }
        }

        public void notifySuccessfulTasksOnUnchangedClusterState() {
            nonFailedTasks.forEach(task -> {
                if (task.listener instanceof AckedClusterStateTaskListener) {
                    //no need to wait for ack if nothing changed, the update can be counted as acknowledged
                    ((AckedClusterStateTaskListener) task.listener).onAllNodesAcked(null);
                }
                task.listener.clusterStateProcessed(task.source(), newClusterState, newClusterState);
            });
        }
    }

    /**
     * Add a listener for updated cluster states
     */
    public void addListener(MasterServiceListener listener) {
        stateListeners.add(listener);
    }

    /**
     * Removes a listener for updated cluster states.
     */
    public void removeListener(MasterServiceListener listener) {
        stateListeners.remove(listener);
    }

    /**
     * Returns the tasks that are pending.
     */
    public List<PendingClusterTask> pendingTasks() {
        return batchingClusterTaskExecutor.pendingTasks();
    }

    /**
     * Returns the number of currently pending tasks.
     */
    public int numberOfPendingTasks() {
        return batchingClusterTaskExecutor.numberOfPendingTasks();
    }

    /**
     * Returns the maximum wait time for tasks in the queue
     *
     * @return A zero time value if the queue is empty, otherwise the time value oldest task waiting in the queue
     */
    public TimeValue getMaxTaskWaitTime() {
        return batchingClusterTaskExecutor.getMaxTaskWaitTime();
    }

    private SafeClusterStateTaskListener safe(PublishingClusterStateTaskListener listener) {
        if (listener instanceof AckedClusterStateTaskListener) {
            return new SafeAckedClusterStateTaskListener((AckedClusterStateTaskListener) listener, logger);
        } else {
            return new SafeClusterStateTaskListener(listener, logger);
        }
    }

    private static class SafeClusterStateTaskListener implements PublishingClusterStateTaskListener {
        private final PublishingClusterStateTaskListener listener;
        private final Logger logger;

        SafeClusterStateTaskListener(PublishingClusterStateTaskListener listener, Logger logger) {
            this.listener = listener;
            this.logger = logger;
        }

        @Override
        public void onFailure(String source, Exception e) {
            try {
                listener.onFailure(source, e);
            } catch (Exception inner) {
                inner.addSuppressed(e);
                logger.error(
                    (Supplier<?>) () -> new ParameterizedMessage(
                        "exception thrown by listener notifying of failure from [{}]", source), inner);
            }
        }

        @Override
        public void onNoLongerMaster(String source) {
            try {
                listener.onNoLongerMaster(source);
            } catch (Exception e) {
                logger.error(
                    (Supplier<?>) () -> new ParameterizedMessage(
                        "exception thrown by listener while notifying no longer master from [{}]", source), e);
            }
        }

        @Override
        public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
            try {
                listener.clusterStateProcessed(source, oldState, newState);
            } catch (Exception e) {
                logger.error(
                    (Supplier<?>) () -> new ParameterizedMessage(
                        "exception thrown by listener while notifying of cluster state processed from [{}], old cluster state:\n" +
                            "{}\nnew cluster state:\n{}",
                        source, oldState, newState),
                    e);
            }
        }
    }

    private static class SafeAckedClusterStateTaskListener extends SafeClusterStateTaskListener implements AckedClusterStateTaskListener {
        private final AckedClusterStateTaskListener listener;
        private final Logger logger;

        SafeAckedClusterStateTaskListener(AckedClusterStateTaskListener listener, Logger logger) {
            super(listener, logger);
            this.listener = listener;
            this.logger = logger;
        }

        @Override
        public boolean mustAck(DiscoveryNode discoveryNode) {
            return listener.mustAck(discoveryNode);
        }

        @Override
        public void onAllNodesAcked(@Nullable Exception e) {
            try {
                listener.onAllNodesAcked(e);
            } catch (Exception inner) {
                inner.addSuppressed(e);
                logger.error("exception thrown by listener while notifying on all nodes acked", inner);
            }
        }

        @Override
        public void onAckTimeout() {
            try {
                listener.onAckTimeout();
            } catch (Exception e) {
                logger.error("exception thrown by listener while notifying on ack timeout", e);
            }
        }

        @Override
        public TimeValue ackTimeout() {
            return listener.ackTimeout();
        }
    }

    protected void warnAboutSlowTaskIfNeeded(TimeValue executionTime, String source) {
        if (executionTime.getMillis() > slowTaskLoggingThreshold.getMillis()) {
            logger.warn("cluster state update task [{}] took [{}] above the warn threshold of {}", source, executionTime,
                slowTaskLoggingThreshold);
        }
    }

    private static class DelegetingAckListener implements Discovery.AckListener {

        private final List<Discovery.AckListener> listeners;

        private DelegetingAckListener(List<Discovery.AckListener> listeners) {
            this.listeners = listeners;
        }

        @Override
        public void onNodeAck(DiscoveryNode node, @Nullable Exception e) {
            for (Discovery.AckListener listener : listeners) {
                listener.onNodeAck(node, e);
            }
        }

        @Override
        public void onTimeout() {
            throw new UnsupportedOperationException("no timeout delegation");
        }
    }

    private static class AckCountDownListener implements Discovery.AckListener {

        private static final Logger logger = Loggers.getLogger(AckCountDownListener.class);

        private final AckedClusterStateTaskListener ackedTaskListener;
        private final CountDown countDown;
        private final DiscoveryNodes nodes;
        private final long clusterStateVersion;
        private final Future<?> ackTimeoutCallback;
        private Exception lastFailure;

        AckCountDownListener(AckedClusterStateTaskListener ackedTaskListener, long clusterStateVersion, DiscoveryNodes nodes,
                             ThreadPool threadPool) {
            this.ackedTaskListener = ackedTaskListener;
            this.clusterStateVersion = clusterStateVersion;
            this.nodes = nodes;
            int countDown = 0;
            for (DiscoveryNode node : nodes) {
                if (ackedTaskListener.mustAck(node)) {
                    countDown++;
                }
            }
            //we always wait for at least 1 node (the master)
            countDown = Math.max(1, countDown);
            logger.trace("expecting {} acknowledgements for cluster_state update (version: {})", countDown, clusterStateVersion);
            this.countDown = new CountDown(countDown);
            this.ackTimeoutCallback = threadPool.schedule(ackedTaskListener.ackTimeout(), ThreadPool.Names.GENERIC, () -> onTimeout());
        }

        @Override
        public void onNodeAck(DiscoveryNode node, @Nullable Exception e) {
            if (!ackedTaskListener.mustAck(node)) {
                //we always wait for the master ack anyway
                if (!node.equals(nodes.getMasterNode())) {
                    return;
                }
            }
            if (e == null) {
                logger.trace("ack received from node [{}], cluster_state update (version: {})", node, clusterStateVersion);
            } else {
                this.lastFailure = e;
                logger.debug(
                    (Supplier<?>) () -> new ParameterizedMessage(
                        "ack received from node [{}], cluster_state update (version: {})", node, clusterStateVersion),
                    e);
            }

            if (countDown.countDown()) {
                logger.trace("all expected nodes acknowledged cluster_state update (version: {})", clusterStateVersion);
                FutureUtils.cancel(ackTimeoutCallback);
                ackedTaskListener.onAllNodesAcked(lastFailure);
            }
        }

        @Override
        public void onTimeout() {
            if (countDown.fastForward()) {
                logger.trace("timeout waiting for acknowledgement for cluster_state update (version: {})", clusterStateVersion);
                ackedTaskListener.onAckTimeout();
            }
        }
    }

    protected <T> ClusterTasksResult<T> executeTasks(TaskInputs<T> taskInputs, long startTimeNS, ClusterState previousClusterState) {
        ClusterTasksResult<T> clusterTasksResult;
        try {
            List<T> inputs = taskInputs.updateTasks.stream().map(tUpdateTask -> tUpdateTask.task).collect(Collectors.toList());
            clusterTasksResult = taskInputs.executor.execute(previousClusterState, inputs);
            if (previousClusterState != clusterTasksResult.resultingState &&
                (clusterTasksResult.resultingState.nodes().isLocalNodeElectedMaster() == false)) {
                assert false : "cluster state update cannot remove master";
                throw new IllegalArgumentException("cluster state update cannot remove master");
            }
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
            clusterTasksResult = ClusterTasksResult.<T>builder()
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
            ClusterTasksResult<T> finalClusterTasksResult = clusterTasksResult;
            taskInputs.updateTasks.forEach(updateTask -> {
                assert finalClusterTasksResult.executionResults.containsKey(updateTask.task) :
                    "missing task result for " + updateTask;
            });
        }

        return clusterTasksResult;
    }

    public <T> List<BatchingUpdateTask<T, PublishingClusterStateTaskListener, ClusterStateTaskExecutor<T>>> getNonFailedTasks(
        TaskInputs<T> taskInputs, ClusterTasksResult<T> clusterTasksResult) {
        return taskInputs.updateTasks.stream().filter(updateTask -> {
            assert clusterTasksResult.executionResults.containsKey(updateTask.task) : "missing " + updateTask;
            final ClusterStateTaskExecutor.TaskResult taskResult =
                clusterTasksResult.executionResults.get(updateTask.task);
            return taskResult.isSuccess();
        }).collect(Collectors.toList());
    }

    /**
     * Represents a set of tasks to be processed together with their executor
     */
    protected class TaskInputs<T> {
        public final String summary;
        public final List<BatchingUpdateTask<T, PublishingClusterStateTaskListener, ClusterStateTaskExecutor<T>>> updateTasks;
        public final ClusterStateTaskExecutor<T> executor;

        TaskInputs(ClusterStateTaskExecutor<T> executor, List<BatchingUpdateTask<T, PublishingClusterStateTaskListener,
            ClusterStateTaskExecutor<T>>> updateTasks, String summary) {
            this.summary = summary;
            this.executor = executor;
            this.updateTasks = updateTasks;
        }

        public boolean runOnlyOnMaster() {
            return executor.runOnlyOnMaster();
        }

        public void onNoLongerMaster() {
            updateTasks.forEach(task -> task.listener.onNoLongerMaster(task.source()));
        }
    }

    protected <T> BatchingClusterTaskExecutor.RunTasks<T, PublishingClusterStateTaskListener, ClusterStateTaskExecutor<T>> runTasks() {
        return (executor, toExecute, tasksSummary) -> runTasks(new TaskInputs<>(executor, toExecute, tasksSummary));
    }

    @Override
    public <T> void submitStateUpdateTasks(final String source,
                                           final Map<T, PublishingClusterStateTaskListener> tasks, final ClusterStateTaskConfig config,
                                           final ClusterStateTaskExecutor<T> executor) {
        if (!lifecycle.started()) {
            return;
        }
        try {
            Map<T, PublishingClusterStateTaskListener> safeTasks = tasks.entrySet().stream().collect(
                Collectors.toMap(Map.Entry::getKey, l -> safe(l.getValue())));
            batchingClusterTaskExecutor.submitTasks(source, safeTasks, config, executor, runTasks());
        } catch (EsRejectedExecutionException e) {
            // ignore cases where we are shutting down..., there is really nothing interesting
            // to be done here...
            if (!lifecycle.stoppedOrClosed()) {
                throw e;
            }
        }
    }

    // this one is overridden in tests so we can control time
    protected long currentTimeInNanos() {
        return System.nanoTime();
    }
}
