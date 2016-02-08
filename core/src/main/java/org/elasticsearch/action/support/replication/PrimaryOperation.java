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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ReplicationResponse;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Responsible for performing primary operation locally or delegating primary operation to relocation target in case where shard has
 * been marked as RELOCATED. Delegates to replication action once successful.
 * <p>
 * Note that as soon as we move to replication action, state responsibility is transferred to {@link ReplicationPhase}.
 */

public class PrimaryOperation<Request extends ReplicationRequest<Request>, ReplicaRequest extends
        ReplicationRequest<ReplicaRequest>, Response extends ReplicationResponse> extends AbstractRunnable {

    protected final Supplier<ClusterState> clusterStateSupplier;
    private final Request request;
    final boolean checkWriteConsistency;
    private final AtomicBoolean finished = new AtomicBoolean();
    final ESLogger logger;
    private final IndexShardReference indexShardReference;
    private final String opType;
    private final ActionListener<Response> listener;
    private final NodeAction<Request, Response> routeToRelocatedPrimaryAction;
    private final Consumer<Tuple<Response, ReplicaRequest>> replicationAction;

    public PrimaryOperation(Request request, boolean checkWriteConsistency, ESLogger logger,
                            IndexShardReference indexShardReference, Supplier<ClusterState> clusterStateSupplier,
                            String opType, ActionListener<Response> listener, NodeAction<Request, Response> routeToRelocatedPrimaryAction,
                            Consumer<Tuple<Response, ReplicaRequest>> replicationAction) {
        this.clusterStateSupplier = clusterStateSupplier;
        this.request = request;
        this.checkWriteConsistency = checkWriteConsistency;
        this.logger = logger;
        this.indexShardReference = indexShardReference;
        this.opType = opType;
        assert this.request.shardId() != null : "request shardId must be set prior to primary phase";
        this.listener = listener;
        this.routeToRelocatedPrimaryAction = routeToRelocatedPrimaryAction;
        this.replicationAction = replicationAction;
    }


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
            listener.onFailure(e);
        } else {
            assert false : "finishAsFailed called but operation is already finished";
        }
    }

    private void finishAsSuccess(Response response) {
        if (finished.compareAndSet(false, true)) {
            listener.onResponse(response);
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
            routeToRelocatedPrimaryAction.execute(relocatingNode, request, ActionListener.wrap(this::finishAsSuccess, this::finishAsFailed));
        } else {
            // execute locally
            Tuple<Response, ReplicaRequest> primaryResponse = indexShardReference.shardOperationOnPrimary(state.metaData(), request);
            if (logger.isTraceEnabled()) {
                logger.trace("action [{}] completed on shard [{}] for request [{}] with cluster state version [{}]", opType,
                        request.shardId(), request, state.version());
            }
            replicationAction.accept(primaryResponse);
        }
    }

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
}
