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
package org.elasticsearch.action.resync;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.action.support.replication.TransportWriteAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.PrimaryReplicaSyncer;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.util.function.Supplier;

public class TransportResyncReplicationAction extends TransportWriteAction<ResyncReplicationRequest,
    ResyncReplicationRequest, ResyncReplicationResponse> implements PrimaryReplicaSyncer.SyncAction {

    public static String ACTION_NAME = "indices:admin/seq_no/resync";

    @Inject
    public TransportResyncReplicationAction(Settings settings, TransportService transportService,
                                               ClusterService clusterService, IndicesService indicesService, ThreadPool threadPool,
                                               ShardStateAction shardStateAction, ActionFilters actionFilters,
                                               IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, ACTION_NAME, transportService, clusterService, indicesService, threadPool, shardStateAction, actionFilters,
            indexNameExpressionResolver, ResyncReplicationRequest::new, ResyncReplicationRequest::new, ThreadPool.Names.BULK);
    }

    @Override
    protected void registerRequestHandlers(String actionName, TransportService transportService, Supplier<ResyncReplicationRequest> request,
                                           Supplier<ResyncReplicationRequest> replicaRequest, String executor) {
        transportService.registerRequestHandler(actionName, request, ThreadPool.Names.SAME, new OperationTransportHandler());
        // we should never reject resync because of thread pool capacity
        transportService.registerRequestHandler(transportPrimaryAction,
            () -> new ConcreteShardRequest<>(request),
            executor, true, true,
            new PrimaryOperationTransportHandler());
        transportService.registerRequestHandler(transportReplicaAction,
            () -> new ConcreteReplicaRequest<>(replicaRequest),
            executor, true, true,
            new ReplicaOperationTransportHandler());
    }

    @Override
    protected ResyncReplicationResponse newResponseInstance() {
        return new ResyncReplicationResponse();
    }

    @Override
    protected WritePrimaryResult<ResyncReplicationRequest, ResyncReplicationResponse> shardOperationOnPrimary(
        ResyncReplicationRequest request, IndexShard primary) throws Exception {
        final ResyncReplicationRequest replicaRequest = performOnPrimary(request, primary);
        return new WritePrimaryResult<>(replicaRequest, new ResyncReplicationResponse(), null, null, primary, logger);
    }

    public static ResyncReplicationRequest performOnPrimary(ResyncReplicationRequest request, IndexShard primary) {
        final ResyncReplicationRequest replicaRequest;
        if (request.getPrimaryAllocationId().equals(primary.routingEntry().allocationId().getId())) {
            replicaRequest = request;
        } else {
            replicaRequest = null; // don't replicate
        }
        return replicaRequest;
    }

    @Override
    protected WriteReplicaResult shardOperationOnReplica(ResyncReplicationRequest request, IndexShard replica) throws Exception {
        Translog.Location location = performOnReplica(request, replica);
        return new WriteReplicaResult(request, location, null, replica, logger);
    }

    public static Translog.Location performOnReplica(ResyncReplicationRequest request, IndexShard replica) throws Exception {
        Translog.Location location = null;
        for (Translog.Operation operation : request.getOperations()) {
            try {
                final Engine.Result operationResult;
                switch (operation.opType()) {
                    case CREATE:
                    case INDEX:
                        final Translog.Index index = (Translog.Index) operation;
                        final SourceToParse sourceToParse =
                            SourceToParse.source(replica.shardId().getIndexName(),
                                index.type(), index.id(), index.source(), XContentFactory.xContentType(index.source()))
                                .routing(index.routing()).parent(index.parent());
                        operationResult = executeIndexOperationOnReplica(index.seqNo(), index.primaryTerm(), index.version(),
                            index.versionType().versionTypeForReplicationAndRecovery(), index.getAutoGeneratedIdTimestamp(), true,
                            sourceToParse, replica);
                        break;
                    case DELETE:
                        final Translog.Delete delete = (Translog.Delete) operation;
                        operationResult = executeDeleteOperationOnReplica(delete.seqNo(), delete.primaryTerm(), delete.version(),
                            delete.type(), delete.id(), delete.versionType().versionTypeForReplicationAndRecovery(), replica);
                        break;
                    case NO_OP:
                        final Translog.NoOp noOp = (Translog.NoOp) operation;
                        operationResult = executeNoOpOnReplica(noOp.seqNo(), noOp.primaryTerm(), noOp.reason(), replica);
                        break;
                    default:
                        throw new IllegalStateException("Unexpected request operation type on replica: " + operation.opType());
                }
                assert operationResult != null : "operation result must never be null";
                location = syncOperationResultOrThrow(operationResult, location);
            } catch (Exception e) {
                // if its not a failure to be ignored, let it bubble up
                if (!TransportActions.isShardNotAvailableException(e)) {
                    throw e;
                }
            }
        }
        return location;
    }

    @Override
    public void sync(ResyncReplicationRequest request, Task parentTask, ActionListener<ResyncReplicationResponse> listener) {
        // skip reroute phase
        transportService.sendChildRequest(
            clusterService.localNode(),
            transportPrimaryAction,
            new ConcreteShardRequest<>(request, request.getPrimaryAllocationId()),
            parentTask,
            transportOptions,
            new TransportResponseHandler<ResyncReplicationResponse>() {
                @Override
                public ResyncReplicationResponse newInstance() {
                    return newResponseInstance();
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }

                @Override
                public void handleResponse(ResyncReplicationResponse response) {
                    listener.onResponse(response);
                }

                @Override
                public void handleException(TransportException exp) {
                    listener.onFailure(exp);
                }
            });
    }

}
