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

package org.elasticsearch.action.support.replication;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ReplicationResponse;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
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
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.elasticsearch.action.support.replication.ReplicaOperation.ignoreReplicaException;

/**
 * Responsible for performing primary operation locally or delegating primary operation to relocation target in case where shard has
 * been marked as RELOCATED. Delegates to replication action once successful.
 * <p>
 * Note that as soon as we move to replication action, state responsibility is transferred to {@link ReplicationPhase}.
 */

public abstract class PrimaryOperation<Request extends ReplicationRequest<Request>, ReplicaRequest extends
        ReplicationRequest<ReplicaRequest>, Response extends ReplicationResponse> extends AbstractRunnable {

    protected final Supplier<ClusterState> clusterStateSupplier;
    private final Request request;
    final boolean checkWriteConsistency;
    private final AtomicBoolean finished = new AtomicBoolean();
    final ESLogger logger;
    private final IndexShardReference indexShardReference;
    private final String opType;


    public PrimaryOperation(Request request, boolean checkWriteConsistency, ESLogger logger,
                            IndexShardReference indexShardReference, Supplier<ClusterState> clusterStateSupplier,
                            String opType) {
        this.clusterStateSupplier = clusterStateSupplier;
        this.request = request;
        this.checkWriteConsistency = checkWriteConsistency;
        this.logger = logger;
        this.indexShardReference = indexShardReference;
        this.opType = opType;
        assert this.request.shardId() != null : "request shardId must be set prior to primary phase";
    }


    protected abstract void finishedAsSuccess(Response response);

    protected abstract void finishedAsFailed(Throwable failure);

    protected abstract void routeToRelocatedPrimary(DiscoveryNode targetNode, Consumer<Response> finishAsSuccess, Consumer<Throwable>
            finishAsFailed);

    protected abstract void performOnReplica(DiscoveryNode node, final ShardRouting shard, ReplicaRequest request, Consumer<ShardRouting>
            onSuccess, Consumer<Exception> onFailure);

    protected abstract void failReplicaOnMaster(ShardRouting shard, ShardRouting primaryRouting, String message, Exception exception,
                                                Runnable onSuccess, Consumer<Throwable> onFailure);

    @Override
    public void onFailure(Throwable e) {
        finishAsFailed(e);
    }

    private void finishAsFailed(Throwable e) {
        if (finished.compareAndSet(false, true)) {
            if (ExceptionsHelper.status(e) == RestStatus.CONFLICT) {
                if (logger.isTraceEnabled()) {
                    logger.trace("failed to execute [{}] on [{}]", e, request, request.shardId());
                }
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("failed to execute [{}] on [{}]", e, request, request.shardId());
                }
            }
            finishedAsFailed(e);
        } else {
            assert false : "finishAsFailed called but operation is already finished";
        }
    }

    private void finishAsSuccess(Response response) {
        if (finished.compareAndSet(false, true)) {
            finishedAsSuccess(response);
        } else {
            assert false : "finishAsSuccess called but operation is already finished";
        }
    }

    @Override
    protected void doRun() throws Exception {
        // request shardID was set in ReroutePhase
        final ClusterState state = clusterStateSupplier.get();
        final String writeConsistencyFailure = checkWriteConsistency(request.shardId(), state);
        if (writeConsistencyFailure != null) {
            throw new UnavailableShardsException(request.shardId(), "{} Timeout: [{}], request: [{}]", writeConsistencyFailure,
                    request.timeout(), request);
        }
        if (indexShardReference.isRelocated()) {
            // delegate primary phase to relocation target
            // it is safe to execute primary phase on relocation target as there are no more in-flight operations where primary
            // phase is executed on local shard and all subsequent operations are executed on relocation target as primary phase.
            final ShardRouting primary = indexShardReference.routingEntry();
            assert primary.relocating() : "indexShard is marked as relocated but routing isn't" + primary;
            DiscoveryNode relocatingNode = state.nodes().get(primary.relocatingNodeId());
            routeToRelocatedPrimary(relocatingNode, this::finishAsSuccess, this::finishAsFailed);
        } else {
            // execute locally
            Tuple<Response, ReplicaRequest> primaryResponse = indexShardReference.shardOperationOnPrimary(state.metaData(), request);
            if (logger.isTraceEnabled()) {
                logger.trace("action [{}] completed on shard [{}] for request [{}] with cluster state version [{}]", opType,
                        request.shardId(), request, state.version());
            }
            ReplicationPhase replicationPhase = new ReplicationPhase(primaryResponse.v2(), primaryResponse.v1());
            moveToReplication(replicationPhase);
        }
    }

    protected void moveToReplication(ReplicationPhase replicationPhase) {replicationPhase.run();}


    /**
     * checks whether we can perform a write based on the write consistency setting
     * returns **null* if OK to proceed, or a string describing the reason to stop
     */
    String checkWriteConsistency(ShardId shardId, ClusterState state) {
        if (checkWriteConsistency == false) {
            return null;
        }

        final WriteConsistencyLevel consistencyLevel = request.consistencyLevel();
        final int sizeActive;
        final int requiredNumber;
        IndexRoutingTable indexRoutingTable = state.getRoutingTable().index(shardId.getIndexName());
        if (indexRoutingTable != null) {
            IndexShardRoutingTable shardRoutingTable = indexRoutingTable.shard(shardId.getId());
            if (shardRoutingTable != null) {
                sizeActive = shardRoutingTable.activeShards().size();
                if (consistencyLevel == WriteConsistencyLevel.QUORUM && shardRoutingTable.getSize() > 2) {
                    // only for more than 2 in the number of shardIt it makes sense, otherwise its 1 shard with 1 replica, quorum is 1
                    // (which is what it is initialized to)
                    requiredNumber = (shardRoutingTable.getSize() / 2) + 1;
                } else if (consistencyLevel == WriteConsistencyLevel.ALL) {
                    requiredNumber = shardRoutingTable.getSize();
                } else {
                    requiredNumber = 1;
                }
            } else {
                sizeActive = 0;
                requiredNumber = 1;
            }
        } else {
            sizeActive = 0;
            requiredNumber = 1;
        }

        if (sizeActive < requiredNumber) {
            logger.trace("not enough active copies of shard [{}] to meet write consistency of [{}] (have {}, needed {}), scheduling a " +
                            "retry. action [{}], request [{}]",
                    shardId, consistencyLevel, sizeActive, requiredNumber, opType, request);
            return "Not enough active copies to meet write consistency of [" + consistencyLevel + "] (have " + sizeActive + ", needed " +
                    requiredNumber + ").";
        } else {
            return null;
        }
    }


    /**
     * Responsible for sending replica requests (see {@link ReplicaOperation}) to nodes with replica copy, including
     * relocating copies
     */
    final class ReplicationPhase implements Runnable {
        private final ReplicaRequest replicaRequest;
        private final Response finalResponse;
        private final List<ShardRouting> shards;
        private final DiscoveryNodes nodes;
        private final boolean executeOnReplica;
        private final AtomicInteger success = new AtomicInteger(1); // We already wrote into the primary shard
        private final ConcurrentMap<String, Throwable> shardReplicaFailures = ConcurrentCollections.newConcurrentMap();
        private final AtomicInteger pending;
        private final int totalShards;

        public ReplicationPhase(ReplicaRequest replicaRequest, Response finalResponse) {
            this.replicaRequest = replicaRequest;
            this.finalResponse = finalResponse;
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
                        pending.get(),
                        opType, replicaRequest, state.version());
            }
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
            performOnReplica(node, shard, replicaRequest, this::onReplicaSuccess, exception -> {
                logger.trace("[{}] failure during replica request [{}], action [{}]", exception, node, replicaRequest, opType);
                if (ignoreReplicaException(exception)) {
                    onReplicaFailure(nodeId, exception);
                } else {
                    String message = String.format(Locale.ROOT, "failed to perform %s on replica on node %s", opType, node);
                    failReplicaOnMaster(
                            shard,
                            indexShardReference.routingEntry(),
                            message,
                            exception,
                            () ->
                                    onReplicaFailure(nodeId, exception),
                            t -> onReplicaFailure(nodeId, exception)
                            // TODO: handle catastrophic non-channel failures
                    );
                }

            });
        }

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
                    finishedAsSuccess(finalResponse);
                }
            } catch (Throwable t) {
                onFailure(t);
            }
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
     * Sets the current phase on the task if it isn't null. Pulled into its own
     * method because its more convenient that way.
     */
    static void setPhase(ReplicationTask task, String phase) {
        if (task != null) {
            task.setPhase(phase);
        }
    }
}
