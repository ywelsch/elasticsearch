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
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskExecutor.ClusterTasksResult;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.NodeConnectionsService;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;

public class DiscoveryService extends AbstractClusterTaskExecutor {

    public static final Setting<TimeValue> CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING =
        Setting.positiveTimeSetting("cluster.service.slow_task_logging_threshold", TimeValue.timeValueSeconds(30),
            Property.Dynamic, Property.NodeScope);

    public static final String MASTER_UPDATE_THREAD_NAME = "masterService#updateTask";

    private BiConsumer<ClusterChangedEvent, Discovery.AckListener> clusterStatePublisher;

    private TimeValue slowTaskLoggingThreshold;

    private final AtomicReference<ClusterState> masterState;

    private final ClusterBlocks.Builder initialBlocks;

    private NodeConnectionsService nodeConnectionsService;

    private DiscoverySettings discoverySettings;

    private final ClusterApplierService clusterApplierService;

    public DiscoveryService(Settings settings, ClusterSettings clusterSettings, ThreadPool threadPool,
                            ClusterApplierService clusterApplierService) {
        super(settings, clusterSettings, threadPool);
        this.clusterApplierService = clusterApplierService;
        // will be replaced on doStart.
        this.masterState = new AtomicReference<>(ClusterState.builder(clusterName).build());
        this.slowTaskLoggingThreshold = CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING.get(settings);

        initialBlocks = ClusterBlocks.builder();
    }

    public void setSlowTaskLoggingThreshold(TimeValue slowTaskLoggingThreshold) {
        this.slowTaskLoggingThreshold = slowTaskLoggingThreshold;
    }

    public synchronized void setClusterStatePublisher(BiConsumer<ClusterChangedEvent, Discovery.AckListener> publisher) {
        clusterStatePublisher = publisher;
    }

    public synchronized void setLocalNode(DiscoveryNode localNode) {
        assert state().nodes().getLocalNodeId() == null : "local node is already set";
        updateState(clusterState -> {
            DiscoveryNodes nodes = DiscoveryNodes.builder(clusterState.nodes()).add(localNode).localNodeId(localNode.getId()).build();
            return ClusterState.builder(clusterState).nodes(nodes).build();
        });
    }

    private void updateState(UnaryOperator<ClusterState> updateFunction) {
        this.masterState.getAndUpdate(updateFunction);
    }

    public synchronized void setNodeConnectionsService(NodeConnectionsService nodeConnectionsService) {
        assert this.nodeConnectionsService == null : "nodeConnectionsService is already set";
        this.nodeConnectionsService = nodeConnectionsService;
    }

    /**
     * Adds an initial block to be set on the first cluster state created.
     */
    public synchronized void addInitialStateBlock(ClusterBlock block) throws IllegalStateException {
        if (lifecycle.started()) {
            throw new IllegalStateException("can't set initial block when started");
        }
        initialBlocks.addGlobalBlock(block);
    }

    /**
     * Remove an initial block to be set on the first cluster state created.
     */
    public synchronized void removeInitialStateBlock(ClusterBlock block) throws IllegalStateException {
        removeInitialStateBlock(block.id());
    }

    /**
     * Remove an initial block to be set on the first cluster state created.
     */
    public synchronized void removeInitialStateBlock(int blockId) throws IllegalStateException {
        if (lifecycle.started()) {
            throw new IllegalStateException("can't set initial block when started");
        }
        initialBlocks.removeGlobalBlock(blockId);
    }

    @Override
    protected synchronized void doStart() {
        Objects.requireNonNull(clusterStatePublisher, "please set a cluster state publisher before starting");
        Objects.requireNonNull(state().nodes().getLocalNode(), "please set the local node before starting");
        Objects.requireNonNull(nodeConnectionsService, "please set the node connection service before starting");
        Objects.requireNonNull(discoverySettings, "please set discovery settings before starting");
        updateState(state -> ClusterState.builder(state).blocks(initialBlocks).build());
        this.threadExecutor = EsExecutors.newSinglePrioritizing(MASTER_UPDATE_THREAD_NAME,
            daemonThreadFactory(settings, MASTER_UPDATE_THREAD_NAME), threadPool.getThreadContext());
    }

    @Override
    protected synchronized void doStop() {
        ThreadPool.terminate(threadExecutor, 10, TimeUnit.SECONDS);
    }

    @Override
    protected synchronized void doClose() {
    }

    /**
     * The local node.
     */
    public DiscoveryNode localNode() {
        DiscoveryNode localNode = state().getNodes().getLocalNode();
        if (localNode == null) {
            throw new IllegalStateException("No local node found. Is the node started?");
        }
        return localNode;
    }

    /**
     * The current cluster state.
     */
    public ClusterState state() {
        return this.masterState.get();
    }

    /** asserts that the current thread is the cluster state update thread */
    public static boolean assertClusterStateThread() {
        assert Thread.currentThread().getName().contains(ClusterService.CLUSTER_UPDATE_THREAD_NAME) :
            "not called from the cluster state update thread";
        return true;
    }

    public static boolean assertMasterStateThread() {
        assert Thread.currentThread().getName().contains(ClusterService.MASTER_UPDATE_THREAD_NAME) :
            "not called from the master state update thread";
        return true;
    }

    public static boolean assertClusterOrMasterStateThread() {
        assert Thread.currentThread().getName().contains(ClusterService.CLUSTER_UPDATE_THREAD_NAME) ||
            Thread.currentThread().getName().contains(ClusterService.MASTER_UPDATE_THREAD_NAME) :
            "not called from the master/cluster state update thread";
        return true;
    }

    public static boolean assertNotMasterUpdateThread(String reason) {
        assert Thread.currentThread().getName().contains(MASTER_UPDATE_THREAD_NAME) == false :
            "Expected current thread [" + Thread.currentThread() + "] to not be the master state update thread. Reason: [" + reason + "]";
        return true;
    }

    public void setDiscoverySettings(DiscoverySettings discoverySettings) {
        this.discoverySettings = discoverySettings;
    }

    @Override
    protected void runTasks(ClusterStateTaskExecutor<Object> executor, ArrayList<UpdateTask> toExecute, String tasksSummary) {
        runTasks(new TaskInputs(executor, toExecute, tasksSummary));
    }

    void runTasks(TaskInputs taskInputs) {
        if (!lifecycle.started()) {
            logger.debug("processing [{}]: ignoring, cluster service not started", taskInputs.summary);
            return;
        }

        logger.debug("processing [{}]: execute", taskInputs.summary);
        final ClusterState previousClusterState = state();

        if (!previousClusterState.nodes().isLocalNodeElectedMaster() && taskInputs.runOnlyOnMaster()) {
            logger.debug("failing [{}]: local node is no longer master", taskInputs.summary);
            taskInputs.onNoLongerMaster();
            return;
        }

        long startTimeNS = currentTimeInNanos();
        TaskOutputs taskOutputs = calculateTaskOutputs(taskInputs, previousClusterState, startTimeNS);
        taskOutputs.notifyFailedTasks();

        if (taskOutputs.clusterStateUnchanged()) {
            taskOutputs.notifySuccessfulTasksOnUnchangedClusterState();
            TimeValue executionTime = TimeValue.timeValueMillis(Math.max(0, TimeValue.nsecToMSec(currentTimeInNanos() - startTimeNS)));
            logger.debug("processing [{}]: took [{}] no change in cluster_state", taskInputs.summary, executionTime);
            warnAboutSlowTaskIfNeeded(executionTime, taskInputs.summary);
        } else {
            ClusterState newClusterState = taskOutputs.newClusterState;
            if (logger.isTraceEnabled()) {
                logger.trace("cluster state updated, source [{}]\n{}", taskInputs.summary, newClusterState);
            } else if (logger.isDebugEnabled()) {
                logger.debug("cluster state updated, version [{}], source [{}]", newClusterState.version(), taskInputs.summary);
            }
            try {
                if (newClusterState.nodes().isLocalNodeElectedMaster()) {
                    publishChanges(taskInputs, taskOutputs);
                } else {
                    masterState.set(newClusterState);
                    CountDownLatch latch = new CountDownLatch(1);
                    clusterApplierService.submitStateUpdateTask("apply-locally-on-master", newClusterState, new ClusterStateTaskListener() {
                        @Override
                        public void onFailure(String source, Exception e) {
                            latch.countDown();
                        }

                        @Override
                        public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                            latch.countDown();
                        }
                    });

                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        logger.error(
                            (Supplier<?>) () -> new ParameterizedMessage(
                                "interrupted while applying cluster state locally [{}]",
                                taskInputs.summary),
                            e);
                    }
                    taskOutputs.processedDifferentClusterState(previousClusterState, newClusterState);
                }
                TimeValue executionTime = TimeValue.timeValueMillis(Math.max(0, TimeValue.nsecToMSec(currentTimeInNanos() - startTimeNS)));
                logger.debug("processing [{}]: took [{}] done publishing updated cluster_state (version: {}, uuid: {})", taskInputs.summary,
                    executionTime, newClusterState.version(),
                    newClusterState.stateUUID());
                warnAboutSlowTaskIfNeeded(executionTime, taskInputs.summary);
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
                        taskInputs.summary,
                        fullState),
                    e);
                // TODO: do we want to call updateTask.onFailure here?
            }
        }
    }

    public TaskOutputs calculateTaskOutputs(TaskInputs taskInputs, ClusterState previousClusterState, long startTimeNS) {
        ClusterTasksResult<Object> clusterTasksResult = executeTasks(taskInputs, startTimeNS, previousClusterState);
        // extract those that are waiting for results
        List<UpdateTask> nonFailedTasks = new ArrayList<>();
        for (UpdateTask updateTask : taskInputs.updateTasks) {
            assert clusterTasksResult.executionResults.containsKey(updateTask.task) : "missing " + updateTask;
            final ClusterStateTaskExecutor.TaskResult taskResult =
                clusterTasksResult.executionResults.get(updateTask.task);
            if (taskResult.isSuccess()) {
                nonFailedTasks.add(updateTask);
            }
        }
        ClusterState newClusterState = patchVersionsAndNoMasterBlocks(taskInputs, previousClusterState, clusterTasksResult);

        return new TaskOutputs(taskInputs, previousClusterState, newClusterState, nonFailedTasks,
            clusterTasksResult.executionResults);
    }

    private ClusterTasksResult<Object> executeTasks(TaskInputs taskInputs, long startTimeNS, ClusterState previousClusterState) {
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

    private ClusterState patchVersionsAndNoMasterBlocks(TaskInputs taskInputs, ClusterState previousClusterState,
                                                        ClusterTasksResult<Object> executionResult) {
        ClusterState newClusterState = executionResult.resultingState;

        if (executionResult.noMaster) {
            assert newClusterState == previousClusterState : "state can only be changed by ClusterService when noMaster = true";
            if (previousClusterState.nodes().getMasterNodeId() != null) {
                // remove block if it already exists before adding new one
                assert previousClusterState.blocks().hasGlobalBlock(discoverySettings.getNoMasterBlock().id()) == false :
                    "NO_MASTER_BLOCK should only be added by ClusterService";
                ClusterBlocks clusterBlocks = ClusterBlocks.builder().blocks(previousClusterState.blocks())
                    .addGlobalBlock(discoverySettings.getNoMasterBlock())
                    .build();

                DiscoveryNodes discoveryNodes = new DiscoveryNodes.Builder(previousClusterState.nodes()).masterNodeId(null).build();
                newClusterState = ClusterState.builder(previousClusterState)
                    .blocks(clusterBlocks)
                    .nodes(discoveryNodes)
                    .build();
            }
        } else if (newClusterState.nodes().isLocalNodeElectedMaster() && taskInputs.isPublishingTask() &&
            previousClusterState != newClusterState) {
            // only the master controls the version numbers
            Builder builder = ClusterState.builder(newClusterState).incrementVersion();
            if (previousClusterState.routingTable() != newClusterState.routingTable()) {
                builder.routingTable(RoutingTable.builder(newClusterState.routingTable())
                    .version(newClusterState.routingTable().version() + 1).build());
            }
            if (previousClusterState.metaData() != newClusterState.metaData()) {
                builder.metaData(MetaData.builder(newClusterState.metaData()).version(newClusterState.metaData().version() + 1));
            }

            // remove the no master block, if it exists
            if (newClusterState.blocks().hasGlobalBlock(discoverySettings.getNoMasterBlock().id())) {
                builder.blocks(ClusterBlocks.builder().blocks(newClusterState.blocks())
                    .removeGlobalBlock(discoverySettings.getNoMasterBlock().id()));
            }

            newClusterState = builder.build();
        }

        assert newClusterState.nodes().getMasterNodeId() == null ||
            newClusterState.blocks().hasGlobalBlock(discoverySettings.getNoMasterBlock().id()) == false :
            "cluster state with master node must not have NO_MASTER_BLOCK";

        return newClusterState;
    }

    private void publishChanges(TaskInputs taskInputs, TaskOutputs taskOutputs) {
        ClusterState previousClusterState = taskOutputs.previousClusterState;
        ClusterState newClusterState = taskOutputs.newClusterState;

        ClusterChangedEvent clusterChangedEvent = new ClusterChangedEvent(taskInputs.summary, newClusterState, previousClusterState);
        // new cluster state, notify all listeners
        final DiscoveryNodes.Delta nodesDelta = clusterChangedEvent.nodesDelta();
        if (nodesDelta.hasChanges() && logger.isInfoEnabled()) {
            String summary = nodesDelta.shortSummary();
            if (summary.length() > 0) {
                logger.info("{}, reason: {}", summary, taskInputs.summary);
            }
        }

        final Discovery.AckListener ackListener = newClusterState.nodes().isLocalNodeElectedMaster() ?
            taskOutputs.createAckListener(threadPool, newClusterState) :
            null;

        nodeConnectionsService.connectToNodes(newClusterState.nodes());

        logger.debug("publishing cluster state version [{}]", newClusterState.version());
        try {
            clusterStatePublisher.accept(clusterChangedEvent, ackListener);
        } catch (Discovery.FailedToCommitClusterStateException t) {
            final long version = newClusterState.version();
            logger.warn(
                (Supplier<?>) () -> new ParameterizedMessage(
                    "failing [{}]: failed to commit cluster state version [{}]", taskInputs.summary, version),
                t);
            // ensure that list of connected nodes in NodeConnectionsService is in-sync with the nodes of the current cluster state
            nodeConnectionsService.connectToNodes(previousClusterState.nodes());
            nodeConnectionsService.disconnectFromNodesExcept(previousClusterState.nodes());
            taskOutputs.publishingFailed(t);
            return;
        }

        masterState.set(newClusterState);


        CountDownLatch latch = new CountDownLatch(1);
        clusterApplierService.submitStateUpdateTask("apply-locally-on-master", newClusterState, new ClusterStateTaskListener() {
            @Override
            public void onFailure(String source, Exception e) {
                latch.countDown();
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                latch.countDown();
            }
        });

        try {
            latch.await();
            try {
                ackListener.onNodeAck(newClusterState.nodes().getLocalNode(), null);
            } catch (Exception e) {
                final DiscoveryNode localNode = newClusterState.nodes().getLocalNode();
                logger.debug(
                    (Supplier<?>) () -> new ParameterizedMessage("error while processing ack for master node [{}]", localNode),
                    e);
            }
        } catch (InterruptedException e) {
            logger.error(
                (Supplier<?>) () -> new ParameterizedMessage(
                    "interrupted while applying cluster state locally [{}]",
                    taskInputs.summary),
                e);
        }

        taskOutputs.processedDifferentClusterState(previousClusterState, newClusterState);

        try {
            taskOutputs.clusterStatePublished(clusterChangedEvent);
        } catch (Exception e) {
            logger.error(
                (Supplier<?>) () -> new ParameterizedMessage(
                    "exception thrown while notifying executor of new cluster state publication [{}]",
                    taskInputs.summary),
                e);
        }
    }

    /**
     * Represents a set of tasks to be processed together with their executor
     */
    class TaskInputs {
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
            return executor.isPublishingTask();
        }

        public void onNoLongerMaster() {
            updateTasks.stream().forEach(task -> task.listener.onNoLongerMaster(task.source));
        }
    }

    /**
     * Output created by executing a set of tasks provided as TaskInputs
     */
    class TaskOutputs {
        public final TaskInputs taskInputs;
        public final ClusterState previousClusterState;
        public final ClusterState newClusterState;
        public final List<UpdateTask> nonFailedTasks;
        public final Map<Object, ClusterStateTaskExecutor.TaskResult> executionResults;

        public TaskOutputs(TaskInputs taskInputs, ClusterState previousClusterState,
                           ClusterState newClusterState, List<UpdateTask> nonFailedTasks,
                           Map<Object, ClusterStateTaskExecutor.TaskResult> executionResults) {
            this.taskInputs = taskInputs;
            this.previousClusterState = previousClusterState;
            this.newClusterState = newClusterState;
            this.nonFailedTasks = nonFailedTasks;
            this.executionResults = executionResults;
        }

        public void publishingFailed(Discovery.FailedToCommitClusterStateException t) {
            nonFailedTasks.forEach(task -> task.listener.onFailure(task.source, t));
        }

        public void processedDifferentClusterState(ClusterState previousClusterState, ClusterState newClusterState) {
            nonFailedTasks.forEach(task -> task.listener.clusterStateProcessed(task.source, previousClusterState, newClusterState));
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
            for (UpdateTask updateTask : taskInputs.updateTasks) {
                assert executionResults.containsKey(updateTask.task) : "missing " + updateTask;
                final ClusterStateTaskExecutor.TaskResult taskResult = executionResults.get(updateTask.task);
                if (taskResult.isSuccess() == false) {
                    updateTask.listener.onFailure(updateTask.source, taskResult.getFailure());
                }
            }
        }

        public void notifySuccessfulTasksOnUnchangedClusterState() {
            nonFailedTasks.forEach(task -> {
                if (task.listener instanceof AckedClusterStateTaskListener) {
                    //no need to wait for ack if nothing changed, the update can be counted as acknowledged
                    ((AckedClusterStateTaskListener) task.listener).onAllNodesAcked(null);
                }
                task.listener.clusterStateProcessed(task.source, newClusterState, newClusterState);
            });
        }
    }

    // this one is overridden in tests so we can control time
    protected long currentTimeInNanos() {
        return System.nanoTime();
    }

    @Override
    protected SafeClusterStateTaskListener safe(ClusterStateTaskListener listener) {
        if (listener instanceof AckedClusterStateTaskListener) {
            return new SafeAckedClusterStateTaskListener((AckedClusterStateTaskListener) listener, logger);
        } else {
            return new SafeClusterStateTaskListener(listener, logger);
        }
    }

    private static class SafeClusterStateTaskListener implements ClusterStateTaskListener {
        private final ClusterStateTaskListener listener;
        private final Logger logger;

        public SafeClusterStateTaskListener(ClusterStateTaskListener listener, Logger logger) {
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

        public SafeAckedClusterStateTaskListener(AckedClusterStateTaskListener listener, Logger logger) {
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

    private void warnAboutSlowTaskIfNeeded(TimeValue executionTime, String source) {
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
}
