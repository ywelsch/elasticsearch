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

import org.elasticsearch.cluster.AckedClusterStateTaskListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterState.Builder;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.NodeConnectionsService;
import org.elasticsearch.cluster.TimeoutClusterStateListener;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.ProcessClusterEventTimeoutException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.service.Batching.BatchedExecution;
import org.elasticsearch.cluster.service.ClusterTaskExecutor.SourcePrioritizedRunnable;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledFuture;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 *
 */
public class ClusterService extends AbstractLifecycleComponent<ClusterService> {

    public static final Setting<TimeValue> CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING =
            Setting.positiveTimeSetting("cluster.service.slow_task_logging_threshold", TimeValue.timeValueSeconds(30),
                    Property.Dynamic, Property.NodeScope);

    private final ThreadPool threadPool;
    private final ClusterName clusterName;

    private BiConsumer<ClusterChangedEvent, Discovery.AckListener> clusterStatePublisher;

    private final OperationRouting operationRouting;

    private final ClusterSettings clusterSettings;

    private TimeValue slowTaskLoggingThreshold;

    private final ClusterTaskExecutor clusterTaskExecutor;

    /**
     * Those 3 state listeners are changing infrequently - CopyOnWriteArrayList is just fine
     */
    private final Collection<ClusterStateListener> priorityClusterStateListeners = new CopyOnWriteArrayList<>();
    private final Collection<ClusterStateListener> clusterStateListeners = new CopyOnWriteArrayList<>();
    private final Collection<ClusterStateListener> lastClusterStateListeners = new CopyOnWriteArrayList<>();
    private final Batching<ClusterStateTaskExecutor, UpdateTask> updateTasksPerExecutor;
    // TODO this is rather frequently changing I guess a Synced Set would be better here and a dedicated remove API
    private final Collection<ClusterStateListener> postAppliedListeners = new CopyOnWriteArrayList<>();
    private final Iterable<ClusterStateListener> preAppliedListeners = Iterables.concat(priorityClusterStateListeners,
            clusterStateListeners, lastClusterStateListeners);

    private final LocalNodeMasterListeners localNodeMasterListeners;

    private final Queue<NotifyTimeout> onGoingTimeouts = ConcurrentCollections.newQueue();

    private volatile ClusterState clusterState;

    private final ClusterBlocks.Builder initialBlocks;

    private NodeConnectionsService nodeConnectionsService;

    public ClusterService(Settings settings,
                          ClusterSettings clusterSettings, ThreadPool threadPool) {
        super(settings);
        this.operationRouting = new OperationRouting(settings, clusterSettings);
        this.threadPool = threadPool;
        this.clusterSettings = clusterSettings;
        this.clusterName = ClusterName.CLUSTER_NAME_SETTING.get(settings);
        // will be replaced on doStart.
        this.clusterState = ClusterState.builder(clusterName).build();

        this.clusterSettings.addSettingsUpdateConsumer(CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING,
                this::setSlowTaskLoggingThreshold);

        this.slowTaskLoggingThreshold = CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING.get(settings);

        localNodeMasterListeners = new LocalNodeMasterListeners(threadPool);

        initialBlocks = ClusterBlocks.builder();

        clusterTaskExecutor = new ClusterTaskExecutor(settings, threadPool);

        updateTasksPerExecutor = new Batching<>(logger);
    }

    private void setSlowTaskLoggingThreshold(TimeValue slowTaskLoggingThreshold) {
        this.slowTaskLoggingThreshold = slowTaskLoggingThreshold;
    }

    synchronized public void setClusterStatePublisher(BiConsumer<ClusterChangedEvent, Discovery.AckListener> publisher) {
        clusterStatePublisher = publisher;
    }

    synchronized public void setLocalNode(DiscoveryNode localNode) {
        assert clusterState.nodes().getLocalNodeId() == null : "local node is already set";
        DiscoveryNodes.Builder nodeBuilder = DiscoveryNodes.builder(clusterState.nodes()).put(localNode).localNodeId(localNode.getId());
        this.clusterState = ClusterState.builder(clusterState).nodes(nodeBuilder).build();
    }

    synchronized public void setNodeConnectionsService(NodeConnectionsService nodeConnectionsService) {
        assert this.nodeConnectionsService == null : "nodeConnectionsService is already set";
        this.nodeConnectionsService = nodeConnectionsService;
    }

    /**
     * Adds an initial block to be set on the first cluster state created.
     */
    synchronized public void addInitialStateBlock(ClusterBlock block) throws IllegalStateException {
        if (lifecycle.started()) {
            throw new IllegalStateException("can't set initial block when started");
        }
        initialBlocks.addGlobalBlock(block);
    }

    /**
     * Remove an initial block to be set on the first cluster state created.
     */
    synchronized public void removeInitialStateBlock(ClusterBlock block) throws IllegalStateException {
        removeInitialStateBlock(block.id());
    }

    /**
     * Remove an initial block to be set on the first cluster state created.
     */
    synchronized public void removeInitialStateBlock(int blockId) throws IllegalStateException {
        if (lifecycle.started()) {
            throw new IllegalStateException("can't set initial block when started");
        }
        initialBlocks.removeGlobalBlock(blockId);
    }

    @Override
    synchronized protected void doStart() {
        Objects.requireNonNull(clusterStatePublisher, "please set a cluster state publisher before starting");
        Objects.requireNonNull(clusterState.nodes().getLocalNode(), "please set the local node before starting");
        Objects.requireNonNull(nodeConnectionsService, "please set the node connection service before starting");
        add(localNodeMasterListeners);
        this.clusterState = ClusterState.builder(clusterState).blocks(initialBlocks).build();
    }

    @Override
    synchronized protected void doStop() {
        for (NotifyTimeout onGoingTimeout : onGoingTimeouts) {
            onGoingTimeout.cancel();
            try {
                onGoingTimeout.cancel();
                onGoingTimeout.listener.onClose();
            } catch (Exception ex) {
                logger.debug("failed to notify listeners on shutdown", ex);
            }
        }
        clusterTaskExecutor.close();
        // close timeout listeners that did not have an ongoing timeout
        postAppliedListeners
                .stream()
                .filter(listener -> listener instanceof TimeoutClusterStateListener)
                .map(listener -> (TimeoutClusterStateListener)listener)
                .forEach(TimeoutClusterStateListener::onClose);
        remove(localNodeMasterListeners);
    }

    @Override
    synchronized protected void doClose() {
    }

    /**
     * The local node.
     */
    public DiscoveryNode localNode() {
        DiscoveryNode localNode = clusterState.getNodes().getLocalNode();
        if (localNode == null) {
            throw new IllegalStateException("No local node found. Is the node started?");
        }
        return localNode;
    }

    public OperationRouting operationRouting() {
        return operationRouting;
    }

    /**
     * The current state.
     */
    public ClusterState state() {
        return this.clusterState;
    }

    /**
     * Adds a priority listener for updated cluster states.
     */
    public void addFirst(ClusterStateListener listener) {
        priorityClusterStateListeners.add(listener);
    }

    /**
     * Adds last listener.
     */
    public void addLast(ClusterStateListener listener) {
        lastClusterStateListeners.add(listener);
    }

    /**
     * Adds a listener for updated cluster states.
     */
    public void add(ClusterStateListener listener) {
        clusterStateListeners.add(listener);
    }

    /**
     * Removes a listener for updated cluster states.
     */
    public void remove(ClusterStateListener listener) {
        clusterStateListeners.remove(listener);
        priorityClusterStateListeners.remove(listener);
        lastClusterStateListeners.remove(listener);
        postAppliedListeners.remove(listener);
        for (Iterator<NotifyTimeout> it = onGoingTimeouts.iterator(); it.hasNext(); ) {
            NotifyTimeout timeout = it.next();
            if (timeout.listener.equals(listener)) {
                timeout.cancel();
                it.remove();
            }
        }
    }

    /**
     * Add a listener for on/off local node master events
     */
    public void add(LocalNodeMasterListener listener) {
        localNodeMasterListeners.add(listener);
    }

    /**
     * Remove the given listener for on/off local master events
     */
    public void remove(LocalNodeMasterListener listener) {
        localNodeMasterListeners.remove(listener);
    }

    /**
     * Adds a cluster state listener that will timeout after the provided timeout,
     * and is executed after the clusterstate has been successfully applied ie. is
     * in state {@link org.elasticsearch.cluster.ClusterState.ClusterStateStatus#APPLIED}
     * NOTE: a {@code null} timeout means that the listener will never be removed
     * automatically
     */
    public void add(@Nullable final TimeValue timeout, final TimeoutClusterStateListener listener) {
        if (lifecycle.stoppedOrClosed()) {
            listener.onClose();
            return;
        }
        // call the post added notification on the same event thread
        try {
            clusterTaskExecutor.execute(new SourcePrioritizedRunnable(Priority.HIGH, "_add_listener_") {
                @Override
                public void run() {
                    if (timeout != null) {
                        NotifyTimeout notifyTimeout = new NotifyTimeout(listener, timeout);
                        notifyTimeout.future = threadPool.schedule(timeout, ThreadPool.Names.GENERIC, notifyTimeout);
                        onGoingTimeouts.add(notifyTimeout);
                    }
                    postAppliedListeners.add(listener);
                    listener.postAdded();
                }
            });
        } catch (EsRejectedExecutionException e) {
            if (lifecycle.stoppedOrClosed()) {
                listener.onClose();
            } else {
                throw e;
            }
        }
    }

    /**
     * Submits a cluster state update task; unlike {@link #submitStateUpdateTask(String, Object, ClusterStateTaskConfig,
     * ClusterStateTaskExecutor, ClusterStateTaskListener)}, submitted updates will not be batched.
     *
     * @param source     the source of the cluster state update task
     * @param updateTask the full context for the cluster state update
     *                   task
     */
    public void submitStateUpdateTask(final String source, final ClusterStateUpdateTask updateTask) {
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
            // convert to an identity map to check for dups based on update tasks semantics of using identity instead of equal
            final IdentityHashMap<T, ClusterStateTaskListener> tasksIdentity = new IdentityHashMap<>(tasks);
            final List<UpdateTask> updateTasks = tasksIdentity.entrySet().stream().map(
                e -> new UpdateTask<>(source, e.getKey(), config, () -> runTasksForExecutor(executor), safe(e.getValue(), logger))
            ).collect(Collectors.toList());

            updateTasksPerExecutor.addToBatches(executor, updateTasks, existing -> {
                if (tasksIdentity.containsKey(existing.task)) {
                    throw new IllegalArgumentException("task [" + existing.task + "] is already queued");
                }
            });

            final UpdateTask<T> firstTask = updateTasks.get(0);

            if (config.timeout() != null) {
                clusterTaskExecutor.execute(firstTask, threadPool.scheduler(), config.timeout(), () -> threadPool.generic().execute(() -> {
                    for (UpdateTask<T> task : updateTasks) {
                        if (task.processed.getAndSet(true) == false) {
                            logger.debug("cluster state update task [{}] timed out after [{}]", source, config.timeout());
                            task.listener.onFailure(source, new ProcessClusterEventTimeoutException(config.timeout(), source));
                        }
                    }
                }));
            } else {
                clusterTaskExecutor.execute(firstTask);
            }
        } catch (EsRejectedExecutionException e) {
            // ignore cases where we are shutting down..., there is really nothing interesting
            // to be done here...
            if (!lifecycle.stoppedOrClosed()) {
                throw e;
            }
        }
    }

    /**
     * Returns task executor that is responsible for executing the cluster state update  tasks
     */
    public ClusterTaskExecutor clusterTaskExecutor() {
        return clusterTaskExecutor;
    }

    public ClusterName getClusterName() {
        return clusterName;
    }

    <T> void runTasksForExecutor(ClusterStateTaskExecutor<T> executor) {
        final BatchedExecution<UpdateTask<T>> batch = (BatchedExecution)updateTasksPerExecutor.getCurrentBatch(executor);
        final ArrayList<UpdateTask<T>> toExecute = batch.toExecute;
        if (batch.isEmpty()) {
            return;
        }
        String source = batch.combinedSource;
        if (!lifecycle.started()) {
            logger.debug("processing [{}]: ignoring, cluster_service not started", source);
            return;
        }
        logger.debug("processing [{}]: execute", source);
        ClusterState previousClusterState = clusterState;
        if (!previousClusterState.nodes().isLocalNodeElectedMaster() && executor.runOnlyOnMaster()) {
            logger.debug("failing [{}]: local node is no longer master", source);
            toExecute.stream().forEach(task -> task.listener.onNoLongerMaster(task.source));
            return;
        }
        ClusterStateTaskExecutor.BatchResult<T> batchResult;
        long startTimeNS = currentTimeInNanos();
        try {
            List<T> inputs = toExecute.stream().map(tUpdateTask -> tUpdateTask.task).collect(Collectors.toList());
            batchResult = executor.execute(previousClusterState, inputs);
        } catch (Throwable e) {
            TimeValue executionTime = TimeValue.timeValueMillis(Math.max(0, TimeValue.nsecToMSec(currentTimeInNanos() - startTimeNS)));
            if (logger.isTraceEnabled()) {
                logger.trace("failed to execute cluster state update in [{}], state:\nversion [{}], source [{}]\n{}{}{}", e, executionTime,
                        previousClusterState.version(), source, previousClusterState.nodes().prettyPrint(),
                        previousClusterState.routingTable().prettyPrint(), previousClusterState.getRoutingNodes().prettyPrint());
            }
            warnAboutSlowTaskIfNeeded(executionTime, source);
            batchResult = ClusterStateTaskExecutor.BatchResult.<T>builder()
                    .failures(toExecute.stream().map(updateTask -> updateTask.task)::iterator, e)
                    .build(previousClusterState);
        }

        assert batchResult.executionResults != null;
        assert batchResult.executionResults.size() == toExecute.size()
                : String.format(Locale.ROOT, "expected [%d] task result%s but was [%d]", toExecute.size(),
                toExecute.size() == 1 ? "" : "s", batchResult.executionResults.size());
        boolean assertsEnabled = false;
        assert (assertsEnabled = true);
        if (assertsEnabled) {
            for (UpdateTask<T> updateTask : toExecute) {
                assert batchResult.executionResults.containsKey(updateTask.task) : "missing task result for [" + updateTask.task + "]";
            }
        }

        ClusterState newClusterState = batchResult.resultingState;
        final ArrayList<UpdateTask<T>> proccessedListeners = new ArrayList<>();
        // fail all tasks that have failed and extract those that are waiting for results
        for (UpdateTask<T> updateTask : toExecute) {
            assert batchResult.executionResults.containsKey(updateTask.task) : "missing " + updateTask.task.toString();
            final ClusterStateTaskExecutor.TaskResult executionResult =
                    batchResult.executionResults.get(updateTask.task);
            executionResult.handle(
                    () -> proccessedListeners.add(updateTask),
                    ex -> {
                        logger.debug("cluster state update task [{}] failed", ex, updateTask.source);
                        updateTask.listener.onFailure(updateTask.source, ex);
                    }
            );
        }

        if (previousClusterState == newClusterState) {
            for (UpdateTask<T> task : proccessedListeners) {
                if (task.listener instanceof AckedClusterStateTaskListener) {
                    //no need to wait for ack if nothing changed, the update can be counted as acknowledged
                    ((AckedClusterStateTaskListener) task.listener).onAllNodesAcked(null);
                }
                task.listener.clusterStateProcessed(task.source, previousClusterState, newClusterState);
            }
            TimeValue executionTime = TimeValue.timeValueMillis(Math.max(0, TimeValue.nsecToMSec(currentTimeInNanos() - startTimeNS)));
            logger.debug("processing [{}]: took [{}] no change in cluster_state", source, executionTime);
            warnAboutSlowTaskIfNeeded(executionTime, source);
            return;
        }

        try {
            ArrayList<Discovery.AckListener> ackListeners = new ArrayList<>();
            if (newClusterState.nodes().isLocalNodeElectedMaster()) {
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
                for (UpdateTask<T> task : proccessedListeners) {
                    if (task.listener instanceof AckedClusterStateTaskListener) {
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
                    }
                }
            }
            final Discovery.AckListener ackListener = new DelegetingAckListener(ackListeners);

            newClusterState.status(ClusterState.ClusterStateStatus.BEING_APPLIED);

            if (logger.isTraceEnabled()) {
                logger.trace("cluster state updated, source [{}]\n{}", source, newClusterState.prettyPrint());
            } else if (logger.isDebugEnabled()) {
                logger.debug("cluster state updated, version [{}], source [{}]", newClusterState.version(), source);
            }

            ClusterChangedEvent clusterChangedEvent = new ClusterChangedEvent(source, newClusterState, previousClusterState);
            // new cluster state, notify all listeners
            final DiscoveryNodes.Delta nodesDelta = clusterChangedEvent.nodesDelta();
            if (nodesDelta.hasChanges() && logger.isInfoEnabled()) {
                String summary = nodesDelta.shortSummary();
                if (summary.length() > 0) {
                    logger.info("{}, reason: {}", summary, source);
                }
            }

            nodeConnectionsService.connectToAddedNodes(clusterChangedEvent);

            // if we are the master, publish the new state to all nodes
            // we publish here before we send a notification to all the listeners, since if it fails
            // we don't want to notify
            if (newClusterState.nodes().isLocalNodeElectedMaster()) {
                logger.debug("publishing cluster state version [{}]", newClusterState.version());
                try {
                    clusterStatePublisher.accept(clusterChangedEvent, ackListener);
                } catch (Discovery.FailedToCommitClusterStateException t) {
                    logger.warn("failing [{}]: failed to commit cluster state version [{}]", t, source, newClusterState.version());
                    proccessedListeners.forEach(task -> task.listener.onFailure(task.source, t));
                    return;
                }
            }

            // update the current cluster state
            clusterState = newClusterState;
            logger.debug("set local cluster state to version {}", newClusterState.version());
            try {
                // nothing to do until we actually recover from the gateway or any other block indicates we need to disable persistency
                if (clusterChangedEvent.state().blocks().disableStatePersistence() == false && clusterChangedEvent.metaDataChanged()) {
                    final Settings incomingSettings = clusterChangedEvent.state().metaData().settings();
                    clusterSettings.applySettings(incomingSettings);
                }
            } catch (Exception ex) {
                logger.warn("failed to apply cluster settings", ex);
            }
            for (ClusterStateListener listener : preAppliedListeners) {
                try {
                    listener.clusterChanged(clusterChangedEvent);
                } catch (Exception ex) {
                    logger.warn("failed to notify ClusterStateListener", ex);
                }
            }

            nodeConnectionsService.disconnectFromRemovedNodes(clusterChangedEvent);

            newClusterState.status(ClusterState.ClusterStateStatus.APPLIED);

            for (ClusterStateListener listener : postAppliedListeners) {
                try {
                    listener.clusterChanged(clusterChangedEvent);
                } catch (Exception ex) {
                    logger.warn("failed to notify ClusterStateListener", ex);
                }
            }

            //manual ack only from the master at the end of the publish
            if (newClusterState.nodes().isLocalNodeElectedMaster()) {
                try {
                    ackListener.onNodeAck(newClusterState.nodes().getLocalNode(), null);
                } catch (Throwable t) {
                    logger.debug("error while processing ack for master node [{}]", t, newClusterState.nodes().getLocalNode());
                }
            }

            for (UpdateTask<T> task : proccessedListeners) {
                task.listener.clusterStateProcessed(task.source, previousClusterState, newClusterState);
            }

            try {
                executor.clusterStatePublished(clusterChangedEvent);
            } catch (Exception e) {
                logger.error("exception thrown while notifying executor of new cluster state publication [{}]", e, source);
            }

            TimeValue executionTime = TimeValue.timeValueMillis(Math.max(0, TimeValue.nsecToMSec(currentTimeInNanos() - startTimeNS)));
            logger.debug("processing [{}]: took [{}] done applying updated cluster_state (version: {}, uuid: {})", source, executionTime,
                    newClusterState.version(), newClusterState.stateUUID());
            warnAboutSlowTaskIfNeeded(executionTime, source);
        } catch (Throwable t) {
            TimeValue executionTime = TimeValue.timeValueMillis(Math.max(0, TimeValue.nsecToMSec(currentTimeInNanos() - startTimeNS)));
            logger.warn("failed to apply updated cluster state in [{}]:\nversion [{}], uuid [{}], source [{}]\n{}", t, executionTime,
                    newClusterState.version(), newClusterState.stateUUID(), source, newClusterState.prettyPrint());
            // TODO: do we want to call updateTask.onFailure here?
        }

    }

    // this one is overridden in tests so we can control time
    protected long currentTimeInNanos() {return System.nanoTime();}

    private static SafeClusterStateTaskListener safe(ClusterStateTaskListener listener, ESLogger logger) {
        if (listener instanceof AckedClusterStateTaskListener) {
            return new SafeAckedClusterStateTaskListener((AckedClusterStateTaskListener) listener, logger);
        } else {
            return new SafeClusterStateTaskListener(listener, logger);
        }
    }

    static class UpdateTask<T> extends Batching.BatchedSourcePrioritizedRunnable {

        public final T task;
        public final ClusterStateTaskConfig config;
        private final Runnable runnable;
        public final ClusterStateTaskListener listener;

        UpdateTask(String source, T task, ClusterStateTaskConfig config, Runnable runnable, ClusterStateTaskListener listener) {
            super(config.priority(), source);
            this.task = task;
            this.config = config;
            this.runnable = runnable;
            this.listener = listener;
        }

        @Override
        public void run() {
            runnable.run();
        }
    }

    private void warnAboutSlowTaskIfNeeded(TimeValue executionTime, String source) {
        if (executionTime.getMillis() > slowTaskLoggingThreshold.getMillis()) {
            logger.warn("cluster state update task [{}] took [{}] above the warn threshold of {}", source, executionTime,
                    slowTaskLoggingThreshold);
        }
    }

    class NotifyTimeout implements Runnable {
        final TimeoutClusterStateListener listener;
        final TimeValue timeout;
        volatile ScheduledFuture future;

        NotifyTimeout(TimeoutClusterStateListener listener, TimeValue timeout) {
            this.listener = listener;
            this.timeout = timeout;
        }

        public void cancel() {
            FutureUtils.cancel(future);
        }

        @Override
        public void run() {
            if (future != null && future.isCancelled()) {
                return;
            }
            if (lifecycle.stoppedOrClosed()) {
                listener.onClose();
            } else {
                listener.onTimeout(this.timeout);
            }
            // note, we rely on the listener to remove itself in case of timeout if needed
        }
    }

    public ClusterSettings getClusterSettings() {
        return clusterSettings;
    }

    public Settings getSettings() {
        return settings;
    }
}
