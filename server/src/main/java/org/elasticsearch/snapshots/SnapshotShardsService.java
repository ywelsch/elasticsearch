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

package org.elasticsearch.snapshots;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress.ShardSnapshotStatus;
import org.elasticsearch.cluster.SnapshotsInProgress.State;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.SnapshotFailedEngineException;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotFailedException;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.SnapshotsInProgress.completed;
import static org.elasticsearch.transport.EmptyTransportResponseHandler.INSTANCE_SAME;

/**
 * This service runs on data and master nodes and controls currently snapshotted shards on these nodes. It is responsible for
 * starting and stopping shard level snapshots
 */
public class SnapshotShardsService extends AbstractLifecycleComponent implements ClusterStateListener, IndexEventListener {

    public static final String UPDATE_SNAPSHOT_STATUS_ACTION_NAME = "internal:cluster/snapshot/update_snapshot_status";

    private final ClusterService clusterService;

    private final IndicesService indicesService;

    private final SnapshotsService snapshotsService;

    private final TransportService transportService;

    private final ThreadPool threadPool;

    private volatile Map<Snapshot, Map<ShardId, IndexShardSnapshotStatus>> shardSnapshots = new HashMap<>();

    private final SnapshotStateExecutor snapshotStateExecutor = new SnapshotStateExecutor();
    private final UpdateSnapshotStatusAction updateSnapshotStatusHandler;

    @Inject
    public SnapshotShardsService(Settings settings, ClusterService clusterService, SnapshotsService snapshotsService, ThreadPool threadPool,
                                 TransportService transportService, IndicesService indicesService,
                                 ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings);
        this.indicesService = indicesService;
        this.snapshotsService = snapshotsService;
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        if (DiscoveryNode.isDataNode(settings)) {
            // this is only useful on the nodes that can hold data
            clusterService.addListener(this);
        }

        // The constructor of UpdateSnapshotStatusAction will register itself to the TransportService.
        this.updateSnapshotStatusHandler = new UpdateSnapshotStatusAction(settings, UPDATE_SNAPSHOT_STATUS_ACTION_NAME,
            transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver);
    }

    @Override
    protected void doStart() {
        assert transportService.getRequestHandler(UPDATE_SNAPSHOT_STATUS_ACTION_NAME) != null;
    }

    @Override
    protected void doStop() {

    }

    @Override
    protected void doClose() {
        clusterService.removeListener(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        synchronized (shardSnapshots) {
            processIndexShardSnapshots(event.state());
        }
    }

    @Override
    public void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
        // abort any snapshots occurring on the soon-to-be closed shard
        synchronized (shardSnapshots) {
            for (Map.Entry<Snapshot, Map<ShardId, IndexShardSnapshotStatus>> snapshotShards : shardSnapshots.entrySet()) {
                Map<ShardId, IndexShardSnapshotStatus> shards = snapshotShards.getValue();
                IndexShardSnapshotStatus indexShardSnapshotStatus = shards.get(shardId);
                if (indexShardSnapshotStatus != null) {
                    logger.debug("[{}] shard closing, abort snapshotting for snapshot [{}]", shardId, snapshotShards.getKey().getSnapshotId());
                    indexShardSnapshotStatus.abortIfNotCompleted("shard is closing, aborting");
                }
            }
        }
    }

    /**
     * Returns status of shards that are snapshotted on the node and belong to the given snapshot
     * <p>
     * This method is executed on data node
     * </p>
     *
     * @param snapshot  snapshot
     * @return map of shard id to snapshot status
     */
    public Map<ShardId, IndexShardSnapshotStatus> currentSnapshotShards(Snapshot snapshot) {
        Map<ShardId, IndexShardSnapshotStatus> snapshotShards = shardSnapshots.get(snapshot);
        if (snapshotShards == null) {
            return null;
        } else {
            // create copy as underlying map is mutable
            return new HashMap<>(snapshotShards);
        }
    }

    void processIndexShardSnapshots(ClusterState clusterState) {
        SnapshotsInProgress snapshotsInProgress = clusterState.custom(SnapshotsInProgress.TYPE);

        // First, go over all entries in the current map and update them with infos from the cluster state
        for (Iterator<Map.Entry<Snapshot, Map<ShardId, IndexShardSnapshotStatus>>> iter = shardSnapshots.entrySet().iterator(); iter.hasNext(); ) {
            final Map.Entry<Snapshot, Map<ShardId, IndexShardSnapshotStatus>> entry = iter.next();
            final Snapshot snapshot = entry.getKey();
            if (snapshotsInProgress != null && snapshotsInProgress.snapshot(snapshot) != null) {
                SnapshotsInProgress.Entry clusterStateSnapshotEntry = snapshotsInProgress.snapshot(snapshot);
                Map<ShardId, IndexShardSnapshotStatus> localShards = entry.getValue();
                for (Iterator<Map.Entry<ShardId, IndexShardSnapshotStatus>> iter2 = localShards.entrySet().iterator(); iter2.hasNext(); ) {
                    Map.Entry<ShardId, IndexShardSnapshotStatus> localStatus = iter2.next();
                    ShardSnapshotStatus clusterStateStatus = clusterStateSnapshotEntry.shards().get(localStatus.getKey());
                    if (clusterStateStatus == null) {
                        localStatus.getValue().abortIfNotCompleted("snapshot has been removed in cluster state, aborting");
                        iter2.remove();
                    } else if (clusterStateStatus.state().completed()) {
                        localStatus.getValue().abortIfNotCompleted("has been completed by master");
                        iter2.remove();
                    } else if (clusterStateStatus.state() == State.ABORTED) {
                        localStatus.getValue().abortIfNotCompleted(clusterStateStatus.reason());
                        // don't remove yet
                        // TODO: should we notify master again?
                    }
                }

                if (localShards.isEmpty()) {
                    iter.remove();
                }
            } else {
                // abort any running snapshots of shards for the removed entry;
                // this could happen if for some reason the cluster state update for aborting
                // running shards is missed, then the snapshot is removed is a subsequent cluster
                // state update, which is being processed here
                for (IndexShardSnapshotStatus snapshotStatus : entry.getValue().values()) {
                    snapshotStatus.abortIfNotCompleted("snapshot has been removed in cluster state, aborting");
                }
                iter.remove();
            }
        }

        // Second, go over all entries in the cluster state that pertain to the current node, and update local node
        if (snapshotsInProgress != null) {
            final String localNodeId = clusterState.nodes().getLocalNodeId();
            for (SnapshotsInProgress.Entry entry : snapshotsInProgress.entries()) {
                final Snapshot snapshot = entry.snapshot();
                final Map<String, IndexId> snapshotIndices = entry.indices().stream().collect(Collectors.toMap(IndexId::getName, Function.identity()));
                final ImmutableOpenMap<ShardId, ShardSnapshotStatus> clusterStateShards = entry.shards();
                for (Iterator<ObjectObjectCursor<ShardId, ShardSnapshotStatus>> iter = clusterStateShards.iterator(); iter.hasNext(); ) {
                    ObjectObjectCursor<ShardId, ShardSnapshotStatus> curr = iter.next();
                    State currState = curr.value.state();
                    if ((currState == State.INIT || currState == State.ABORTED) && localNodeId.equals(curr.value.nodeId())) {
                        // only INIT and ABORTED are relevant
                        ShardId shardId = curr.key;
                        final IndexShardSnapshotStatus localStatus = shardSnapshots.getOrDefault(entry.snapshot(), Collections.emptyMap()).get(shardId);

                        if (currState == State.INIT) {
                            // we should be snapshotting this

                            if (localStatus == null) {
                                // check if indexshard is available
                                final IndexShard indexShard = indicesService.getShardOrNull(shardId);
                                if (indexShard == null) {
                                    // do nothing, as shard lifecycle will take care of failing this
                                    // should we still notify master for BWC reasons?
                                } else {
                                    // add new snapshot
                                    logger.trace("[{}] - Adding shard to the queue", shardId);
                                    final IndexShardSnapshotStatus indexShardSnapshotStatus = IndexShardSnapshotStatus.newInitializing();
                                    final IndexId indexId = snapshotIndices.get(shardId.getIndexName());
                                    assert indexId != null;
                                    shardSnapshots.computeIfAbsent(entry.snapshot(), s -> new HashMap<>()).put(curr.key, indexShardSnapshotStatus);

                                    threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(new AbstractRunnable() {

                                        final SetOnce<Exception> failure = new SetOnce<>();

                                        @Override
                                        public void doRun() {
                                            snapshot(indexShard, snapshot, indexId, indexShardSnapshotStatus);
                                        }

                                        @Override
                                        public void onFailure(Exception e) {
                                            logger.warn(() -> new ParameterizedMessage("[{}][{}] failed to snapshot shard", shardId, snapshot), e);
                                            failure.set(e);
                                        }

                                        @Override
                                        public void onRejection(Exception e) {
                                            failure.set(e);
                                        }

                                        @Override
                                        public void onAfter() {
                                            final Exception exception = failure.get();
                                            if (exception != null) {
                                                indexShardSnapshotStatus.moveToFailed(System.currentTimeMillis(), ExceptionsHelper.detailedMessage(exception));
                                                notifyFailedSnapshotShard(snapshot, shardId, localNodeId, ExceptionsHelper.detailedMessage(exception));
                                            } else {
                                                notifySuccessfulSnapshotShard(snapshot, shardId, localNodeId);
                                            }
                                        }
                                    });
                                }
                            } else {
                                // depending on status here, notify master again?
                                // or is that taken care of by the first iteration?
                            }
                        } else {
                            assert currState == State.ABORTED;
                            // check if shard has been aborted, and notify master
                            if (localStatus == null) {
                                // due to CS batching we might have missed the INIT state and straight went into ABORTED
                                // notify master that abort has completed by moving to FAILED
                                notifyFailedSnapshotShard(snapshot, shardId, localNodeId, curr.value.reason());
                            } else {
                                // should have been taken care of by the first phase
                            }
                        }

                    }
                }
            }
        }

    }


    /**
     * Creates shard snapshot
     *
     * @param snapshot       snapshot
     * @param snapshotStatus snapshot status
     */
    private void snapshot(final IndexShard indexShard, final Snapshot snapshot, final IndexId indexId, final IndexShardSnapshotStatus snapshotStatus) {
        final ShardId shardId = indexShard.shardId();
        if (indexShard.routingEntry().primary() == false) {
            throw new IndexShardSnapshotFailedException(shardId, "snapshot should be performed only on primary");
        }
        if (indexShard.routingEntry().relocating()) {
            // do not snapshot when in the process of relocation of primaries so we won't get conflicts
            throw new IndexShardSnapshotFailedException(shardId, "cannot snapshot while relocating");
        }

        final IndexShardState indexShardState = indexShard.state();
        if (indexShardState == IndexShardState.CREATED || indexShardState == IndexShardState.RECOVERING) {
            // shard has just been created, or still recovering
            throw new IndexShardSnapshotFailedException(shardId, "shard didn't fully recover yet");
        }

        final Repository repository = snapshotsService.getRepositoriesService().repository(snapshot.getRepository());
        try {
            // we flush first to make sure we get the latest writes snapshotted
            try (Engine.IndexCommitRef snapshotRef = indexShard.acquireLastIndexCommit(true)) {
                repository.snapshotShard(indexShard, snapshot.getSnapshotId(), indexId, snapshotRef.getIndexCommit(), snapshotStatus);
                if (logger.isDebugEnabled()) {
                    final IndexShardSnapshotStatus.Copy lastSnapshotStatus = snapshotStatus.asCopy();
                    logger.debug("snapshot ({}) completed to {} with {}", snapshot, repository, lastSnapshotStatus);
                }
            }
        } catch (SnapshotFailedEngineException | IndexShardSnapshotFailedException e) {
            throw e;
        } catch (Exception e) {
            throw new IndexShardSnapshotFailedException(shardId, "Failed to snapshot", e);
        }
    }

    /**
     * Internal request that is used to send changes in snapshot status to master
     */
    public static class UpdateIndexShardSnapshotStatusRequest extends MasterNodeRequest<UpdateIndexShardSnapshotStatusRequest> {
        private Snapshot snapshot;
        private ShardId shardId;
        private ShardSnapshotStatus status;

        public UpdateIndexShardSnapshotStatusRequest() {

        }

        public UpdateIndexShardSnapshotStatusRequest(Snapshot snapshot, ShardId shardId, ShardSnapshotStatus status) {
            this.snapshot = snapshot;
            this.shardId = shardId;
            this.status = status;
            // By default, we keep trying to post snapshot status messages to avoid snapshot processes getting stuck.
            this.masterNodeTimeout = TimeValue.timeValueNanos(Long.MAX_VALUE);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            snapshot = new Snapshot(in);
            shardId = ShardId.readShardId(in);
            status = new ShardSnapshotStatus(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            snapshot.writeTo(out);
            shardId.writeTo(out);
            status.writeTo(out);
        }

        public Snapshot snapshot() {
            return snapshot;
        }

        public ShardId shardId() {
            return shardId;
        }

        public ShardSnapshotStatus status() {
            return status;
        }

        @Override
        public String toString() {
            return snapshot + ", shardId [" + shardId + "], status [" + status.state() + "]";
        }
    }

    /** Notify the master node that the given shard has been successfully snapshotted **/
    void notifySuccessfulSnapshotShard(final Snapshot snapshot, final ShardId shardId, final String localNodeId) {
        sendSnapshotShardUpdate(snapshot, shardId, new ShardSnapshotStatus(localNodeId, State.SUCCESS));
    }

    /** Notify the master node that the given shard failed to be snapshotted **/
    void notifyFailedSnapshotShard(final Snapshot snapshot, final ShardId shardId, final String localNodeId, final String failure) {
        sendSnapshotShardUpdate(snapshot, shardId, new ShardSnapshotStatus(localNodeId, State.FAILED, failure));
    }

    /** Updates the shard snapshot status by sending a {@link UpdateIndexShardSnapshotStatusRequest} to the master node */
    void sendSnapshotShardUpdate(final Snapshot snapshot, final ShardId shardId, final ShardSnapshotStatus status) {
        try {
            UpdateIndexShardSnapshotStatusRequest request = new UpdateIndexShardSnapshotStatusRequest(snapshot, shardId, status);
            transportService.sendRequest(transportService.getLocalNode(), UPDATE_SNAPSHOT_STATUS_ACTION_NAME, request, INSTANCE_SAME);
        } catch (Exception e) {
            logger.warn(() -> new ParameterizedMessage("[{}] [{}] failed to update snapshot state", snapshot, status), e);
        }
    }

    /**
     * Updates the shard status on master node
     *
     * @param request update shard status request
     */
    private void innerUpdateSnapshotState(final UpdateIndexShardSnapshotStatusRequest request, ActionListener<UpdateIndexShardSnapshotStatusResponse> listener) {
        logger.trace("received updated snapshot restore state [{}]", request);
        clusterService.submitStateUpdateTask(
            "update snapshot state",
            request,
            ClusterStateTaskConfig.build(Priority.NORMAL),
            snapshotStateExecutor,
            new ClusterStateTaskListener() {
                @Override
                public void onFailure(String source, Exception e) {
                    listener.onFailure(e);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    listener.onResponse(new UpdateIndexShardSnapshotStatusResponse());
                }
            });
    }

    class SnapshotStateExecutor implements ClusterStateTaskExecutor<UpdateIndexShardSnapshotStatusRequest> {

        @Override
        public ClusterTasksResult<UpdateIndexShardSnapshotStatusRequest> execute(ClusterState currentState, List<UpdateIndexShardSnapshotStatusRequest> tasks) throws Exception {
            final SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE);
            if (snapshots != null) {
                int changedCount = 0;
                final List<SnapshotsInProgress.Entry> entries = new ArrayList<>();
                for (SnapshotsInProgress.Entry entry : snapshots.entries()) {
                    if (entry.state().completed()) {
                        continue;
                    }
                    ImmutableOpenMap.Builder<ShardId, ShardSnapshotStatus> shards = null;

                    for (UpdateIndexShardSnapshotStatusRequest updateSnapshotState : tasks) {
                        if (entry.snapshot().equals(updateSnapshotState.snapshot())) {
                            final ShardSnapshotStatus currentEntry = entry.shards().get(updateSnapshotState.shardId());
                            if (currentEntry != null) {
                                if (currentEntry.state().completed() == false) {
                                    logger.trace("[{}] Updating shard [{}] with status [{}]", updateSnapshotState.snapshot(), updateSnapshotState.shardId(), updateSnapshotState.status().state());
                                    if (shards == null) {
                                        shards = ImmutableOpenMap.builder(entry.shards());
                                    }
                                    shards.put(updateSnapshotState.shardId(), updateSnapshotState.status());
                                    changedCount++;
                                }
                            } else {
                                // entry should not just have vanished
                                assert false;
                            }
                        }
                    }

                    if (shards != null) {
                        if (completed(shards.values()) == false) {
                            entries.add(new SnapshotsInProgress.Entry(entry, shards.build()));
                        } else {
                            // Snapshot is finished - mark it as done
                            // TODO: Add PARTIAL_SUCCESS status?
                            SnapshotsInProgress.Entry updatedEntry = new SnapshotsInProgress.Entry(entry, State.SUCCESS, shards.build(),
                                null);
                            entries.add(updatedEntry);
                        }
                    } else {
                        entries.add(entry);
                    }
                }
                if (changedCount > 0) {
                    logger.trace("changed cluster state triggered by {} snapshot state updates", changedCount);

                    final SnapshotsInProgress updatedSnapshots = new SnapshotsInProgress(entries.toArray(new SnapshotsInProgress.Entry[entries.size()]));
                    return ClusterTasksResult.<UpdateIndexShardSnapshotStatusRequest>builder().successes(tasks).build(
                        ClusterState.builder(currentState).putCustom(SnapshotsInProgress.TYPE, updatedSnapshots).build());
                }
            }
            return ClusterTasksResult.<UpdateIndexShardSnapshotStatusRequest>builder().successes(tasks).build(currentState);
        }
    }

    static class UpdateIndexShardSnapshotStatusResponse extends ActionResponse {

    }

    class UpdateSnapshotStatusAction extends TransportMasterNodeAction<UpdateIndexShardSnapshotStatusRequest, UpdateIndexShardSnapshotStatusResponse> {
        UpdateSnapshotStatusAction(Settings settings, String actionName, TransportService transportService, ClusterService clusterService,
                                   ThreadPool threadPool, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
            super(settings, actionName, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver, UpdateIndexShardSnapshotStatusRequest::new);
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        protected UpdateIndexShardSnapshotStatusResponse newResponse() {
            return new UpdateIndexShardSnapshotStatusResponse();
        }

        @Override
        protected void masterOperation(UpdateIndexShardSnapshotStatusRequest request, ClusterState state, ActionListener<UpdateIndexShardSnapshotStatusResponse> listener) throws Exception {
            innerUpdateSnapshotState(request, listener);
        }

        @Override
        protected ClusterBlockException checkBlock(UpdateIndexShardSnapshotStatusRequest request, ClusterState state) {
            return null;
        }
    }

}
