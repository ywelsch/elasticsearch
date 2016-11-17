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
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterState.Builder;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskExecutor.BatchResult;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.LocalClusterUpdateTask;
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
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.common.util.concurrent.PrioritizedRunnable;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;

public class ClusterService extends AbstractLifecycleComponent {

    public static final Setting<TimeValue> CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING =
            Setting.positiveTimeSetting("cluster.service.slow_task_logging_threshold", TimeValue.timeValueSeconds(30),
                    Property.Dynamic, Property.NodeScope);

    public static final String UPDATE_THREAD_NAME = "clusterService#updateTask";
    private final ThreadPool threadPool;
    private final ClusterName clusterName;

    private BiConsumer<ClusterChangedEvent, Discovery.AckListener> clusterStatePublisher;

    private final OperationRouting operationRouting;

    private final ClusterSettings clusterSettings;

    private TimeValue slowTaskLoggingThreshold;

    private volatile PrioritizedEsThreadPoolExecutor updateTasksExecutor;

    /**
     * Those 3 state listeners are changing infrequently - CopyOnWriteArrayList is just fine
     */
    private final Collection<ClusterStateListener> priorityClusterStateListeners = new CopyOnWriteArrayList<>();
    private final Collection<ClusterStateListener> clusterStateListeners = new CopyOnWriteArrayList<>();
    private final Collection<ClusterStateListener> lastClusterStateListeners = new CopyOnWriteArrayList<>();
    final Map<ClusterStateTaskExecutor, LinkedHashSet<UpdateTask>> updateTasksPerExecutor = new HashMap<>();
    // TODO this is rather frequently changing I guess a Synced Set would be better here and a dedicated remove API
    private final Collection<ClusterStateListener> postAppliedListeners = new CopyOnWriteArrayList<>();
    private final Iterable<ClusterStateListener> preAppliedListeners = Iterables.concat(priorityClusterStateListeners,
            clusterStateListeners, lastClusterStateListeners);

    private final LocalNodeMasterListeners localNodeMasterListeners;

    private final Queue<NotifyTimeout> onGoingTimeouts = ConcurrentCollections.newQueue();

    private final AtomicReference<ClusterServiceState> state;

    private final ClusterBlocks.Builder initialBlocks;

    private NodeConnectionsService nodeConnectionsService;

    private final DiscoverySettings discoverySettings;

    public ClusterService(Settings settings,
                          ClusterSettings clusterSettings, ThreadPool threadPool) {
        super(settings);
        this.operationRouting = new OperationRouting(settings, clusterSettings);
        this.threadPool = threadPool;
        this.clusterSettings = clusterSettings;
        this.clusterName = ClusterName.CLUSTER_NAME_SETTING.get(settings);
        // will be replaced on doStart.
        this.state = new AtomicReference<>(new ClusterServiceState(ClusterState.builder(clusterName).build(), ClusterStateStatus.UNKNOWN));

        this.clusterSettings.addSettingsUpdateConsumer(CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING,
                this::setSlowTaskLoggingThreshold);

        this.slowTaskLoggingThreshold = CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING.get(settings);

        discoverySettings = new DiscoverySettings(settings, clusterSettings);

        localNodeMasterListeners = new LocalNodeMasterListeners(threadPool);

        initialBlocks = ClusterBlocks.builder();
    }

    private void setSlowTaskLoggingThreshold(TimeValue slowTaskLoggingThreshold) {
        this.slowTaskLoggingThreshold = slowTaskLoggingThreshold;
    }

    public synchronized void setClusterStatePublisher(BiConsumer<ClusterChangedEvent, Discovery.AckListener> publisher) {
        clusterStatePublisher = publisher;
    }

    public synchronized void setLocalNode(DiscoveryNode localNode) {
        assert clusterServiceState().getClusterState().nodes().getLocalNodeId() == null : "local node is already set";
        updateState(css -> {
            ClusterState clusterState = css.getClusterState();
            DiscoveryNodes nodes = DiscoveryNodes.builder(clusterState.nodes()).add(localNode).localNodeId(localNode.getId()).build();
            return new ClusterServiceState(ClusterState.builder(clusterState).nodes(nodes).build(), css.getClusterStateStatus());
        });
    }

    private void updateState(UnaryOperator<ClusterServiceState> updateFunction) {
        this.state.getAndUpdate(oldClusterServiceState -> {
            ClusterServiceState newClusterServiceState = updateFunction.apply(oldClusterServiceState);
            assert validStateTransition(oldClusterServiceState, newClusterServiceState) :
                "Invalid cluster service state transition from " + oldClusterServiceState + " to " + newClusterServiceState;
            return newClusterServiceState;
        });
    }

    private static boolean validStateTransition(ClusterServiceState oldClusterServiceState, ClusterServiceState newClusterServiceState) {
        if (oldClusterServiceState == null || newClusterServiceState == null) {
            return false;
        }
        ClusterStateStatus oldStatus = oldClusterServiceState.getClusterStateStatus();
        ClusterStateStatus newStatus = newClusterServiceState.getClusterStateStatus();
        // only go from UNKNOWN to UNKNOWN or BEING_APPLIED
        if (oldStatus == ClusterStateStatus.UNKNOWN && newStatus == ClusterStateStatus.APPLIED) {
            return false;
        }
        // only go from BEING_APPLIED to APPLIED
        if (oldStatus == ClusterStateStatus.BEING_APPLIED && newStatus != ClusterStateStatus.APPLIED) {
            return false;
        }
        // only go from APPLIED to BEING_APPLIED
        if (oldStatus == ClusterStateStatus.APPLIED && newStatus != ClusterStateStatus.BEING_APPLIED) {
            return false;
        }
        boolean identicalClusterState = oldClusterServiceState.getClusterState() == newClusterServiceState.getClusterState();
        return identicalClusterState == (oldStatus == ClusterStateStatus.BEING_APPLIED && newStatus == ClusterStateStatus.APPLIED);
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
        Objects.requireNonNull(clusterServiceState().getClusterState().nodes().getLocalNode(), "please set the local node before starting");
        Objects.requireNonNull(nodeConnectionsService, "please set the node connection service before starting");
        add(localNodeMasterListeners);
        updateState(css -> new ClusterServiceState(
            ClusterState.builder(css.getClusterState()).blocks(initialBlocks).build(),
            css.getClusterStateStatus()));
        this.updateTasksExecutor = EsExecutors.newSinglePrioritizing(UPDATE_THREAD_NAME, daemonThreadFactory(settings, UPDATE_THREAD_NAME),
                threadPool.getThreadContext());
    }

    @Override
    protected synchronized void doStop() {
        for (NotifyTimeout onGoingTimeout : onGoingTimeouts) {
            onGoingTimeout.cancel();
            try {
                onGoingTimeout.cancel();
                onGoingTimeout.listener.onClose();
            } catch (Exception ex) {
                logger.debug("failed to notify listeners on shutdown", ex);
            }
        }
        ThreadPool.terminate(updateTasksExecutor, 10, TimeUnit.SECONDS);
        // close timeout listeners that did not have an ongoing timeout
        postAppliedListeners
                .stream()
                .filter(listener -> listener instanceof TimeoutClusterStateListener)
                .map(listener -> (TimeoutClusterStateListener)listener)
                .forEach(TimeoutClusterStateListener::onClose);
        remove(localNodeMasterListeners);
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

    public OperationRouting operationRouting() {
        return operationRouting;
    }

    /**
     * The current cluster state.
     */
    public ClusterState state() {
        return clusterServiceState().getClusterState();
    }

    /**
     * The current cluster service state comprising cluster state and cluster state status.
     */
    public ClusterServiceState clusterServiceState() {
        return this.state.get();
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
     * in state {@link ClusterStateStatus#APPLIED}
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
            updateTasksExecutor.execute(new SourcePrioritizedRunnable(Priority.HIGH, "_add_listener_") {
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
     *
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
     * @param ex       the cluster state update task executor; tasks
     *                 that share the same executor will be executed
     *                 batches on this executor
     * @param <T>      the type of the cluster state update task state
     *
     */
    public <T> void submitStateUpdateTasks(final String source,
                                           final Map<T, ClusterStateTaskListener> tasks, final ClusterStateTaskConfig config,
                                           final ClusterStateTaskExecutor<T> ex) {
        if (!lifecycle.started()) {
            return;
        }
        if (tasks.isEmpty()) {
            return;
        }
        try {
            @SuppressWarnings("unchecked")
            ClusterStateTaskExecutor<Object> executor = (ClusterStateTaskExecutor<Object>) ex;
            // convert to an identity map to check for dups based on update tasks semantics of using identity instead of equal
            final IdentityHashMap<Object, ClusterStateTaskListener> tasksIdentity = new IdentityHashMap<>(tasks);
            final List<UpdateTask> updateTasks = tasksIdentity.entrySet().stream().map(
                entry -> new UpdateTask(source, entry.getKey(), config.priority(), executor, safe(entry.getValue(), logger))
            ).collect(Collectors.toList());

            synchronized (updateTasksPerExecutor) {
                LinkedHashSet<UpdateTask> existingTasks = updateTasksPerExecutor.computeIfAbsent(executor,
                    k -> new LinkedHashSet<>(updateTasks.size()));
                for (UpdateTask existing : existingTasks) {
                    if (tasksIdentity.containsKey(existing.task)) {
                        throw new IllegalStateException("task [" + executor.describeTasks(Collections.singletonList(existing.task)) +
                            "] with source [" + source + "] is already queued");
                    }
                }
                existingTasks.addAll(updateTasks);
            }

            final UpdateTask firstTask = updateTasks.get(0);

            final TimeValue timeout = config.timeout();
            if (timeout != null) {
                updateTasksExecutor.execute(firstTask, threadPool.scheduler(), timeout, () -> threadPool.generic().execute(() -> {
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
                }));
            } else {
                updateTasksExecutor.execute(firstTask);
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
     * Returns the tasks that are pending.
     */
    public List<PendingClusterTask> pendingTasks() {
        PrioritizedEsThreadPoolExecutor.Pending[] pendings = updateTasksExecutor.getPending();
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
        return updateTasksExecutor.getNumberOfPendingTasks();
    }

    /**
     * Returns the maximum wait time for tasks in the queue
     *
     * @return A zero time value if the queue is empty, otherwise the time value oldest task waiting in the queue
     */
    public TimeValue getMaxTaskWaitTime() {
        return updateTasksExecutor.getMaxTaskWaitTime();
    }

    /** asserts that the current thread is the cluster state update thread */
    public static boolean assertClusterStateThread() {
        assert Thread.currentThread().getName().contains(ClusterService.UPDATE_THREAD_NAME) :
                "not called from the cluster state update thread";
        return true;
    }

    /** asserts that the current thread is <b>NOT</b> the cluster state update thread */
    public static boolean assertNotClusterStateUpdateThread(String reason) {
        assert Thread.currentThread().getName().contains(UPDATE_THREAD_NAME) == false :
            "Expected current thread [" + Thread.currentThread() + "] to not be the cluster state update thread. Reason: [" + reason + "]";
        return true;
    }

    public ClusterName getClusterName() {
        return clusterName;
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

    class TaskInputs {
        public final String summary;
        public final ArrayList<UpdateTask> updateTasks;
        public final ClusterStateTaskExecutor<Object> executor;

        TaskInputs(ClusterStateTaskExecutor<Object> executor, ArrayList<UpdateTask> updateTasks, String summary) {
            this.summary = summary;
            this.executor = executor;
            this.updateTasks = updateTasks;
        }

        public boolean skipProcessing() {
            return updateTasks.isEmpty();
        }

        public boolean runOnlyOnMaster() {
            return executor.runOnlyOnMaster();
        }

        public void onNoLongerMaster() {
            updateTasks.stream().forEach(task -> task.listener.onNoLongerMaster(task.source));
        }
    }

    class TaskOutputs {
        public final TaskInputs taskInputs;
        public final ClusterServiceState previousClusterServiceState;
        public final ClusterServiceState newClusterServiceState;
        public final List<UpdateTask> nonFailedTasks;

        public TaskOutputs(TaskInputs taskInputs, ClusterServiceState previousClusterServiceState,
                           ClusterServiceState newClusterServiceState, List<UpdateTask> nonFailedTasks) {
            this.taskInputs = taskInputs;
            this.previousClusterServiceState = previousClusterServiceState;
            this.newClusterServiceState = newClusterServiceState;
            this.nonFailedTasks = nonFailedTasks;
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

        public boolean unchanged() {
            return previousClusterServiceState.getClusterState() == newClusterServiceState.getClusterState();
        }
    }

    public TaskOutputs calculateTaskOutputs(TaskInputs taskInputs, ClusterServiceState previousClusterServiceState, long startTimeNS) {
        logger.debug("processing [{}]: execute", taskInputs.summary);
        ClusterState previousClusterState = previousClusterServiceState.getClusterState();

        if (!previousClusterState.nodes().isLocalNodeElectedMaster() && taskInputs.runOnlyOnMaster()) {
            logger.debug("failing [{}]: local node is no longer master", taskInputs.summary);
            taskInputs.onNoLongerMaster();
            return new TaskOutputs(taskInputs, previousClusterServiceState, previousClusterServiceState, Collections.emptyList());
        }

        List<UpdateTask> nonFailedTasks = new ArrayList<>();
        LocalClusterUpdateTask.LocalResult executionResult = executeTasks(taskInputs, startTimeNS, previousClusterState, nonFailedTasks);

        ClusterServiceState newClusterServiceState = patchBlocksAndVersions(previousClusterState, executionResult.clusterState,
            executionResult.offMaster);
        ClusterState newClusterState = newClusterServiceState.getClusterState();

        if (previousClusterState == newClusterState) {
            nonFailedTasks.forEach(task -> {
                if (task.listener instanceof AckedClusterStateTaskListener) {
                    //no need to wait for ack if nothing changed, the update can be counted as acknowledged
                    ((AckedClusterStateTaskListener) task.listener).onAllNodesAcked(null);
                }
                task.listener.clusterStateProcessed(task.source, newClusterState, newClusterState);
            });
            TimeValue executionTime = TimeValue.timeValueMillis(Math.max(0, TimeValue.nsecToMSec(currentTimeInNanos() - startTimeNS)));
            logger.debug("processing [{}]: took [{}] no change in cluster_state", taskInputs.summary, executionTime);
            warnAboutSlowTaskIfNeeded(executionTime, taskInputs.summary);
        } else {
            if (logger.isTraceEnabled()) {
                logger.trace("cluster state updated, source [{}]\n{}", taskInputs.summary, newClusterState);
            } else if (logger.isDebugEnabled()) {
                logger.debug("cluster state updated, version [{}], source [{}]", newClusterState.version(), taskInputs.summary);
            }
        }

        return new TaskOutputs(taskInputs, previousClusterServiceState, newClusterServiceState, nonFailedTasks);
    }

    private void logFailedExecution(TaskInputs taskInputs, long startTimeNS, ClusterState previousClusterState, Exception e) {
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
    }

    private LocalClusterUpdateTask.LocalResult executeTasks(TaskInputs taskInputs, long startTimeNS, ClusterState previousClusterState,
                                                            List<UpdateTask> nonFailedTasks) {
        BatchResult<Object> batchResult;
        boolean localUpdate = ((ClusterStateTaskExecutor) taskInputs.executor) instanceof LocalClusterUpdateTask;
        try {
            List<Object> inputs = taskInputs.updateTasks.stream().map(tUpdateTask -> tUpdateTask.task).collect(Collectors.toList());
            if (localUpdate) {
                LocalClusterUpdateTask localTask = (LocalClusterUpdateTask) ((ClusterStateTaskExecutor) taskInputs.executor);
                LocalClusterUpdateTask.LocalResult executionResult = localTask.executeLocally(previousClusterState);
                ClusterState updatedState = executionResult.clusterState == null ? previousClusterState : executionResult.clusterState;
                batchResult = BatchResult.builder().successes(inputs).build(updatedState);
            } else {
                batchResult = taskInputs.executor.execute(previousClusterState, inputs);
            }
        } catch (Exception e) {
            logFailedExecution(taskInputs, startTimeNS, previousClusterState, e);
            batchResult = BatchResult.builder()
                .failures(taskInputs.updateTasks.stream().map(updateTask -> updateTask.task)::iterator, e)
                .build(previousClusterState);
        }

        assert batchResult.executionResults != null;
        assert batchResult.executionResults.size() == taskInputs.updateTasks.size()
            : String.format(Locale.ROOT, "expected [%d] task result%s but was [%d]", taskInputs.updateTasks.size(),
            taskInputs.updateTasks.size() == 1 ? "" : "s", batchResult.executionResults.size());
        boolean assertsEnabled = false;
        assert (assertsEnabled = true);
        if (assertsEnabled) {
            for (UpdateTask updateTask : taskInputs.updateTasks) {
                assert batchResult.executionResults.containsKey(updateTask.task) :
                    "missing task result for " + updateTask;
            }
        }


        // fail all tasks that have failed and extract those that are waiting for results
        for (UpdateTask updateTask : taskInputs.updateTasks) {
            assert batchResult.executionResults.containsKey(updateTask.task) : "missing " + updateTask;
            final ClusterStateTaskExecutor.TaskResult taskResult =
                batchResult.executionResults.get(updateTask.task);
            taskResult.handle(
                () -> nonFailedTasks.add(updateTask),
                ex -> updateTask.listener.onFailure(updateTask.source, ex)
            );
        }
        return LocalClusterUpdateTask.newState(batchResult.resultingState);
    }

    private ClusterServiceState patchBlocksAndVersions(ClusterState previousClusterState, ClusterState newClusterState, boolean offMaster) {
        if (offMaster && previousClusterState.nodes().isLocalNodeElectedMaster()) {
            assert previousClusterState == newClusterState;

            ClusterBlocks clusterBlocks = ClusterBlocks.builder().blocks(previousClusterState.blocks())
                .removeGlobalBlock(discoverySettings.getNoMasterBlock().id()) // remove block if it already exists before adding new one
                .addGlobalBlock(discoverySettings.getNoMasterBlock())
                .build();

            DiscoveryNodes discoveryNodes = new DiscoveryNodes.Builder(previousClusterState.nodes()).masterNodeId(null).build();
            newClusterState = ClusterState.builder(previousClusterState)
                .blocks(clusterBlocks)
                .nodes(discoveryNodes)
                .build();
            return new ClusterServiceState(newClusterState, ClusterStateStatus.BEING_APPLIED, previousClusterState);
        }

        if (offMaster == false && previousClusterState != newClusterState) {
            // remove the no master block, if it exists
            if (newClusterState.blocks().hasGlobalBlock(discoverySettings.getNoMasterBlock().id())) {
                ClusterBlocks.Builder clusterBlocks = ClusterBlocks.builder().blocks(newClusterState.blocks())
                    .removeGlobalBlock(discoverySettings.getNoMasterBlock().id());
                newClusterState = ClusterState.builder(newClusterState).blocks(clusterBlocks).build();
            }

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
            }
        }
        return new ClusterServiceState(newClusterState, ClusterStateStatus.BEING_APPLIED);
    }

    private void publishAndApplyChanges(TaskOutputs taskOutputs, long startTimeNS) {
        TaskInputs taskInputs = taskOutputs.taskInputs;
        ClusterState previousClusterState = taskOutputs.previousClusterServiceState.getClusterState();
        ClusterState newClusterState = taskOutputs.newClusterServiceState.getClusterState();

        try {
            final Discovery.AckListener ackListener = newClusterState.nodes().isLocalNodeElectedMaster() ?
                taskOutputs.createAckListener(threadPool, newClusterState) :
                null;

            ClusterChangedEvent clusterChangedEvent = new ClusterChangedEvent(taskInputs.summary, newClusterState, previousClusterState);
            // new cluster state, notify all listeners
            final DiscoveryNodes.Delta nodesDelta = clusterChangedEvent.nodesDelta();
            if (nodesDelta.hasChanges() && logger.isInfoEnabled()) {
                String summary = nodesDelta.shortSummary();
                if (summary.length() > 0) {
                    logger.info("{}, reason: {}", summary, taskInputs.summary);
                }
            }

            nodeConnectionsService.connectToNodes(clusterChangedEvent.nodesDelta().addedNodes());

            // if we are the master, publish the new state to all nodes
            // we publish here before we send a notification to all the listeners, since if it fails
            // we don't want to notify
            if (newClusterState.nodes().isLocalNodeElectedMaster()) {
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
                    nodeConnectionsService.disconnectFromNodes(clusterChangedEvent.nodesDelta().addedNodes());
                    taskOutputs.publishingFailed(t);
                    return;
                }
            }

            // update the current cluster state
            updateState(css -> taskOutputs.newClusterServiceState);
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
                    logger.trace("calling [{}] with change to version [{}]", listener, newClusterState.version());
                    listener.clusterChanged(clusterChangedEvent);
                } catch (Exception ex) {
                    logger.warn("failed to notify ClusterStateListener", ex);
                }
            }

            nodeConnectionsService.disconnectFromNodes(clusterChangedEvent.nodesDelta().removedNodes());

            updateState(css -> new ClusterServiceState(css.getClusterState(), ClusterStateStatus.APPLIED));

            for (ClusterStateListener listener : postAppliedListeners) {
                try {
                    logger.trace("calling [{}] with change to version [{}]", listener, newClusterState.version());
                    listener.clusterChanged(clusterChangedEvent);
                } catch (Exception ex) {
                    logger.warn("failed to notify ClusterStateListener", ex);
                }
            }

            //manual ack only from the master at the end of the publish
            if (newClusterState.nodes().isLocalNodeElectedMaster()) {
                try {
                    ackListener.onNodeAck(newClusterState.nodes().getLocalNode(), null);
                } catch (Exception e) {
                    final DiscoveryNode localNode = newClusterState.nodes().getLocalNode();
                    logger.debug(
                        (Supplier<?>) () -> new ParameterizedMessage("error while processing ack for master node [{}]", localNode),
                        e);
                }
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

            TimeValue executionTime = TimeValue.timeValueMillis(Math.max(0, TimeValue.nsecToMSec(currentTimeInNanos() - startTimeNS)));
            logger.debug("processing [{}]: took [{}] done applying updated cluster_state (version: {}, uuid: {})", taskInputs.summary,
                executionTime, newClusterState.version(), newClusterState.stateUUID());
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

    void runTasks(TaskInputs taskInputs) {
        if (taskInputs.skipProcessing()) {
            return;
        }
        if (!lifecycle.started()) {
            logger.debug("processing [{}]: ignoring, cluster service not started", taskInputs.summary);
            return;
        }

        long startTimeNS = currentTimeInNanos();
        TaskOutputs taskOutputs = calculateTaskOutputs(taskInputs, clusterServiceState(), startTimeNS);
        if (taskOutputs.unchanged()) {
            return;
        }
        publishAndApplyChanges(taskOutputs, startTimeNS);
    }

    // this one is overridden in tests so we can control time
    protected long currentTimeInNanos() {return System.nanoTime();}

    private static SafeClusterStateTaskListener safe(ClusterStateTaskListener listener, Logger logger) {
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

    public enum TaskType {
        STEP_DOWN_AS_MASTER,
        UPDATE_CLUSTER_STATE,
    }

    class UpdateTask extends SourcePrioritizedRunnable {

        public final Object task;
        public final ClusterStateTaskListener listener;
        private final ClusterStateTaskExecutor<Object> executor;
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

                final String tasksSummary = processTasksBySource.entrySet().stream().map(entry -> {
                    String tasks = executor.describeTasks(entry.getValue());
                    return tasks.isEmpty() ? entry.getKey() : entry.getKey() + "[" + tasks + "]";
                }).reduce((s1, s2) -> s1 + ", " + s2).orElse("");

                runTasks(new TaskInputs(executor, toExecute, tasksSummary));
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

    private static class LocalNodeMasterListeners implements ClusterStateListener {

        private final List<LocalNodeMasterListener> listeners = new CopyOnWriteArrayList<>();
        private final ThreadPool threadPool;
        private volatile boolean master = false;

        private LocalNodeMasterListeners(ThreadPool threadPool) {
            this.threadPool = threadPool;
        }

        @Override
        public void clusterChanged(ClusterChangedEvent event) {
            if (!master && event.localNodeMaster()) {
                master = true;
                for (LocalNodeMasterListener listener : listeners) {
                    Executor executor = threadPool.executor(listener.executorName());
                    executor.execute(new OnMasterRunnable(listener));
                }
                return;
            }

            if (master && !event.localNodeMaster()) {
                master = false;
                for (LocalNodeMasterListener listener : listeners) {
                    Executor executor = threadPool.executor(listener.executorName());
                    executor.execute(new OffMasterRunnable(listener));
                }
            }
        }

        private void add(LocalNodeMasterListener listener) {
            listeners.add(listener);
        }

        private void remove(LocalNodeMasterListener listener) {
            listeners.remove(listener);
        }

        private void clear() {
            listeners.clear();
        }
    }

    private static class OnMasterRunnable implements Runnable {

        private final LocalNodeMasterListener listener;

        private OnMasterRunnable(LocalNodeMasterListener listener) {
            this.listener = listener;
        }

        @Override
        public void run() {
            listener.onMaster();
        }
    }

    private static class OffMasterRunnable implements Runnable {

        private final LocalNodeMasterListener listener;

        private OffMasterRunnable(LocalNodeMasterListener listener) {
            this.listener = listener;
        }

        @Override
        public void run() {
            listener.offMaster();
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
            this.ackTimeoutCallback = threadPool.schedule(ackedTaskListener.ackTimeout(), ThreadPool.Names.GENERIC, new Runnable() {
                @Override
                public void run() {
                    onTimeout();
                }
            });
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

    public ClusterSettings getClusterSettings() {
        return clusterSettings;
    }

    public Settings getSettings() {
        return settings;
    }
}
