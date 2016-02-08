package org.elasticsearch.action.support.replication;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ReplicationResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.elasticsearch.action.support.replication.ReplicaOperation.ignoreReplicaException;

/**
 * Responsible for sending replica requests (see {@link ReplicaOperation}) to nodes with replica copy, including
 * relocating copies
 */
abstract class ReplicationPhase<ReplicaRequest extends
    ReplicationRequest<ReplicaRequest>, Response extends ReplicationResponse> implements Runnable {
    private final ESLogger logger;
    private final ReplicaRequest replicaRequest;
    private final Response finalResponse;
    private final ActionListener<Response> listener;
    private final List<ShardRouting> shards;
    private final DiscoveryNodes nodes;
    private final boolean executeOnReplica;
    private final AtomicInteger success = new AtomicInteger(1); // We already wrote into the primary shard
    private final ConcurrentMap<String, Throwable> shardReplicaFailures = ConcurrentCollections.newConcurrentMap();
    private final AtomicInteger pending;
    private final int totalShards;
    private final NodeAction<ReplicaRequest, Void> performOnReplicaAction;
    private final AtomicBoolean finished = new AtomicBoolean();
    private final String opType;

    public ReplicationPhase(ESLogger logger, Supplier<ClusterState> clusterStateSupplier, String opType, ReplicaRequest replicaRequest, Response finalResponse, ActionListener<Response> listener,
                            NodeAction<ReplicaRequest, Void> performOnReplicaAction) {
        this.logger = logger;
        this.opType = opType;
        this.replicaRequest = replicaRequest;
        this.finalResponse = finalResponse;
        this.listener = listener;
        this.performOnReplicaAction = performOnReplicaAction;
        ShardId shardId = replicaRequest.shardId();

        // we have to get a new state after successfully indexing into the primary in order to honour recovery semantics.
        // we have to make sure that every operation indexed into the primary after recovery start will also be replicated
        // to the recovery target. If we use an old cluster state, we may miss a relocation that has started since then.
        // If the index gets deleted after primary operation, we skip replication
        final ClusterState state = clusterStateSupplier.get();
        final IndexRoutingTable index = state.getRoutingTable().index(shardId.getIndex());
        final IndexShardRoutingTable shardRoutingTable = (index != null) ? index.shard(shardId.id()) : null;
        final IndexMetaData indexMetaData = state.getMetaData().index(shardId.getIndex());
        this.shards = (shardRoutingTable != null) ? shardRoutingTable.shards() : Collections.emptyList();
        this.executeOnReplica = (indexMetaData == null) || shouldExecuteReplication(indexMetaData.getSettings());
        this.nodes = state.getNodes();

        if (shards.isEmpty()) {
            logger.debug("replication phase for request [{}] on [{}] is skipped due to index deletion after primary operation",
                    replicaRequest, shardId);
        }

        // we calculate number of target nodes to send replication operations, including nodes with relocating shards
        int numberOfIgnoredShardInstances = 0;
        int numberOfPendingShardInstances = 0;
        for (ShardRouting shard : shards) {
            // the following logic to select the shards to replicate to is mirrored and explained in the doRun method below
            if (shard.primary() == false && executeOnReplica == false) {
                numberOfIgnoredShardInstances++;
                continue;
            }
            if (shard.unassigned()) {
                numberOfIgnoredShardInstances++;
                continue;
            }
            if (nodes.localNodeId().equals(shard.currentNodeId()) == false) {
                numberOfPendingShardInstances++;
            }
            if (shard.relocating() && nodes.localNodeId().equals(shard.relocatingNodeId()) == false) {
                numberOfPendingShardInstances++;
            }
        }
        // one for the local primary copy
        this.totalShards = 1 + numberOfPendingShardInstances + numberOfIgnoredShardInstances;
        this.pending = new AtomicInteger(numberOfPendingShardInstances);
        if (logger.isTraceEnabled()) {
            logger.trace("replication phase started. pending [{}], action [{}], request [{}], cluster state version used [{}]",
                    pending.get(), opType, replicaRequest, state.version());
        }
    }

    /**
     * Indicated whether this operation should be replicated to shadow replicas or not. If this method returns true the replication phase
     * will be skipped.
     * For example writes such as index and delete don't need to be replicated on shadow replicas but refresh and flush do.
     */
    protected boolean shouldExecuteReplication(Settings settings) {
        return IndexMetaData.isIndexUsingShadowReplicas(settings) == false;
    }

    /**
     * total shard copies
     */
    int totalShards() {
        return totalShards;
    }

    /**
     * total successful operations so far
     */
    int successful() {
        return success.get();
    }

    /**
     * number of pending operations
     */
    int pending() {
        return pending.get();
    }

    /**
     * start sending replica requests to target nodes
     */
    @Override
    public void run() {
        if (pending.get() == 0) {
            finishReplication();
            return;
        }
        for (ShardRouting shard : shards) {
            if (shard.primary() == false && executeOnReplica == false) {
                // If the replicas use shadow replicas, there is no reason to
                // perform the action on the replica, so skip it and
                // immediately return

                // this delays mapping updates on replicas because they have
                // to wait until they get the new mapping through the cluster
                // state, which is why we recommend pre-defined mappings for
                // indices using shadow replicas
                continue;
            }
            if (shard.unassigned()) {
                continue;
            }
            // we index on a replica that is initializing as well since we might not have got the event
            // yet that it was started. We will get an exception IllegalShardState exception if its not started
            // and that's fine, we will ignore it

            // we never execute replication operation locally as primary operation has already completed locally
            // hence, we ignore any local shard for replication
            if (nodes.localNodeId().equals(shard.currentNodeId()) == false) {
                resolveAndForward(shard);
            }
            // send operation to relocating shard
            // local shard can be a relocation target of a primary that is in relocated state
            if (shard.relocating() && nodes.localNodeId().equals(shard.relocatingNodeId()) == false) {
                resolveAndForward(shard.buildTargetRelocatingShard());
            }
        }
    }

    /**
     * send replica operation to target node
     */
    void resolveAndForward(final ShardRouting shard) {
        // if we don't have that node, it means that it might have failed and will be created again, in
        // this case, we don't have to do the operation, and just let it failover
        String nodeId = shard.currentNodeId();
        if (!nodes.nodeExists(nodeId)) {
            logger.trace("failed to send action [{}] on replica [{}] for request [{}] due to unknown node [{}]", opType, shard
                    .shardId(), replicaRequest, nodeId);
            onReplicaFailure(nodeId, null);
            return;
        }
        if (logger.isTraceEnabled()) {
            logger.trace("perform [{}] on replica [{}] for request [{}] to [{}]", opType, shard.shardId(), replicaRequest, nodeId);
        }

        final DiscoveryNode node = nodes.get(nodeId);
        performOnReplicaAction.execute(node, replicaRequest, new ActionListener<Void>() {
                    @Override
                    public void onResponse(Void aVoid) {
                        onReplicaSuccess(shard);
                    }

                    @Override
                    public void onFailure(Throwable exception) {
                        logger.trace("[{}] failure during replica request [{}], action [{}]", exception, node, replicaRequest, opType);
                        if (ignoreReplicaException(exception)) {
                            onReplicaFailure(nodeId, exception);
                        } else {
                            String message = String.format(Locale.ROOT, "failed to perform %s on replica on node %s", opType, node);
                            failReplicaOnMaster(
                                    shard,
                                    message,
                                    exception,
                                    new ShardStateAction.Listener() {
                                        @Override
                                        public void onSuccess() {
                                            onReplicaFailure(nodeId, exception);
                                        }

                                        @Override
                                        public void onFailure(Throwable t) {
                                            // TODO: handle catastrophic non-channel failures
                                            onReplicaFailure(nodeId, exception);
                                        }
                                    }
                            );
                        }
                    }
                }
        );
    }

    protected abstract void failReplicaOnMaster(ShardRouting shard, String message, Throwable exception, ShardStateAction.Listener callback);

    void onReplicaFailure(String nodeId, @Nullable Throwable e) {
        // Only version conflict should be ignored from being put into the _shards header?
        if (e != null && ignoreReplicaException(e) == false) {
            shardReplicaFailures.put(nodeId, e);
        }
        decPendingAndFinishIfNeeded();
    }

    void onReplicaSuccess(ShardRouting routing) {
        success.incrementAndGet();
        decPendingAndFinishIfNeeded();
    }

    private void decPendingAndFinishIfNeeded() {
        if (pending.decrementAndGet() <= 0) {
            finishReplication();
        }
    }

    private void finishReplication() {
        try {
            final ReplicationResponse.ShardInfo.Failure[] failuresArray;
            if (!shardReplicaFailures.isEmpty()) {
                int slot = 0;
                failuresArray = new ReplicationResponse.ShardInfo.Failure[shardReplicaFailures.size()];
                for (Map.Entry<String, Throwable> entry : shardReplicaFailures.entrySet()) {
                    RestStatus restStatus = ExceptionsHelper.status(entry.getValue());
                    failuresArray[slot++] = new ReplicationResponse.ShardInfo.Failure(replicaRequest.shardId(), entry.getKey(),
                            entry.getValue(), restStatus, false);
                }
            } else {
                failuresArray = ReplicationResponse.EMPTY;
            }
            finalResponse.setShardInfo(new ReplicationResponse.ShardInfo(
                            totalShards,
                            success.get(),
                            failuresArray

                    )
            );
            if (finished.compareAndSet(false, true)) {
                listener.onResponse(finalResponse);
            }
        } catch (Throwable t) {
            listener.onFailure(t);
        }
    }
}
