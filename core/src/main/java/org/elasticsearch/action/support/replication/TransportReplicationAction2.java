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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ReplicationResponse;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.EmptyTransportResponseHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * Base class for requests that should be executed on a primary copy followed by replica copies.
 * Subclasses can resolve the target shard and provide implementation for primary and replica operations.
 *
 * The action samples cluster state on the receiving node to reroute to node with primary copy and on the
 * primary node to validate request before primary operation followed by sampling state again for resolving
 * nodes with replica copies to perform replication.
 */
public abstract class TransportReplicationAction2<Request extends ReplicationRequest<Request>, ReplicaRequest extends
        ReplicationRequest<ReplicaRequest>, Response extends ReplicationResponse> extends TransportAction<Request, Response> {

    protected final TransportService transportService;
    protected final ClusterService clusterService;
    protected final IndicesService indicesService;
    protected final ShardStateAction shardStateAction;
    protected final WriteConsistencyLevel defaultWriteConsistencyLevel;
    protected final TransportRequestOptions transportOptions;
    protected final MappingUpdatedAction mappingUpdatedAction;

    final String transportReplicaAction;
    final String transportPrimaryAction;
    final String executor;
    final boolean checkWriteConsistency;

    protected TransportReplicationAction2(Settings settings, String actionName, TransportService transportService,
                                          ClusterService clusterService, IndicesService indicesService,
                                          ThreadPool threadPool, ShardStateAction shardStateAction,
                                          MappingUpdatedAction mappingUpdatedAction, ActionFilters actionFilters,
                                          IndexNameExpressionResolver indexNameExpressionResolver, Supplier<Request> request,
                                          Supplier<ReplicaRequest> replicaRequest, String executor) {
        super(settings, actionName, threadPool, actionFilters, indexNameExpressionResolver, transportService.getTaskManager());
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.shardStateAction = shardStateAction;
        this.mappingUpdatedAction = mappingUpdatedAction;

        this.transportPrimaryAction = actionName + "[p]";
        this.transportReplicaAction = actionName + "[r]";
        this.executor = executor;
        this.checkWriteConsistency = checkWriteConsistency();
        transportService.registerRequestHandler(actionName, request, ThreadPool.Names.SAME, new OperationTransportHandler());
        transportService.registerRequestHandler(transportPrimaryAction, request, executor, new PrimaryOperationTransportHandler());
        // we must never reject on because of thread pool capacity on replicas
        transportService.registerRequestHandler(transportReplicaAction, replicaRequest, executor, true, new
                ReplicaOperationTransportHandler());

        this.transportOptions = transportOptions();

        this.defaultWriteConsistencyLevel = WriteConsistencyLevel.fromString(settings.get("action.write_consistency", "quorum"));
    }

    @Override
    protected final void doExecute(Request request, ActionListener<Response> listener) {
        throw new UnsupportedOperationException("the task parameter is required for this operation");
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        final ReplicationTask replicationTask = (ReplicationTask) task;
        setPhase(replicationTask, "routing");
        final ClusterStateObserver observer = new ClusterStateObserver(clusterService, request.timeout(), logger,
                threadPool.getThreadContext());
        ClusterState state = observer.observedState();
        final String concreteIndex = resolveIndex() ? indexNameExpressionResolver.concreteSingleIndex(state, request) : request.index();
        // request does not have a shardId yet, we need to pass the concrete index to resolve shardId
        resolveRequest(state.metaData(), concreteIndex, request);
        assert request.shardId() != null : "request shardId must be set in resolveRequest";

        if (request.consistencyLevel() == WriteConsistencyLevel.DEFAULT) {
            request.consistencyLevel(defaultWriteConsistencyLevel);
        }

        new PrimaryReroute<Request, Response>(request, observer, getBlockLevel(), actionName, logger) {
            @Override
            protected void performPrimaryOperation(DiscoveryNode node, Request request, ActionListener<Response> callback) {
                setPhase(replicationTask, "waiting_on_primary");
                transportService.sendRequest(node, transportPrimaryAction, request, transportOptions,
                        ActionListenerResponseHandler.responseHandler(callback, TransportReplicationAction2.this::newResponseInstance));
            }

            @Override
            protected void finishedAsReroute(DiscoveryNode node, Request request) {
                setPhase(replicationTask, "rerouted");
                transportService.sendRequest(node, actionName, request, transportOptions,
                        ActionListenerResponseHandler.responseHandler(listener, TransportReplicationAction2.this::newResponseInstance));
            }

            @Override
            protected void finishedAsSuccess(Response response) {
                setPhase(replicationTask, "finished");
                listener.onResponse(response);
            }

            @Override
            protected void finishedAsFailed(Throwable t) {
                setPhase(replicationTask, "failed");
                listener.onFailure(t);
            }
        }.run();
    }

    protected abstract Response newResponseInstance();

    /**
     * Resolves the target shard id of the incoming request.
     * Additional processing or validation of the request should be done here.
     */
    protected void resolveRequest(MetaData metaData, String concreteIndex, Request request) {
        // implementation should be provided if request shardID is not already resolved at request construction
    }

    /**
     * True if write consistency should be checked for an implementation
     */
    protected boolean checkWriteConsistency() {
        return true;
    }

    /**
     * Cluster level block to check before request execution
     */
    protected ClusterBlockLevel getBlockLevel() {
        return ClusterBlockLevel.WRITE;
    }


    /**
     * True if provided index should be resolved when resolving request
     */
    protected boolean resolveIndex() {
        return true;
    }

    protected TransportRequestOptions transportOptions() {
        return TransportRequestOptions.EMPTY;
    }


    protected static class WriteResult<T extends ReplicationResponse> {

        public final T response;
        public final Translog.Location location;

        public WriteResult(T response, Translog.Location location) {
            this.response = response;
            this.location = location;
        }

        @SuppressWarnings("unchecked")
        public <T extends ReplicationResponse> T response() {
            // this sets total, pending and failed to 0 and this is ok, because we will embed this into the replica
            // request and not use it
            response.setShardInfo(new ReplicationResponse.ShardInfo());
            return (T) response;
        }

    }

    class OperationTransportHandler implements TransportRequestHandler<Request> {
        @Override
        public void messageReceived(final Request request, final TransportChannel channel, Task task) throws Exception {
            execute(task, request, new ActionListener<Response>() {
                @Override
                public void onResponse(Response result) {
                    try {
                        channel.sendResponse(result);
                    } catch (Throwable e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    try {
                        channel.sendResponse(e);
                    } catch (Throwable e1) {
                        logger.warn("Failed to send response for " + actionName, e1);
                    }
                }
            });
        }

        @Override
        public void messageReceived(Request request, TransportChannel channel) throws Exception {
            throw new UnsupportedOperationException("the task parameter is required for this operation");
        }
    }

    class PrimaryOperationTransportHandler implements TransportRequestHandler<Request> {
        @Override
        public void messageReceived(final Request request, final TransportChannel channel) throws Exception {
            throw new UnsupportedOperationException("the task parameter is required for this operation");
        }

        @Override
        public void messageReceived(Request request, TransportChannel channel, Task task) throws Exception {
            ReplicationTask replicationTask = (ReplicationTask) task;
            setPhase(replicationTask, "primary");
            IndexShardReference<Request, ReplicaRequest, Response> shard = getIndexShardReferenceOnPrimary(request.shardId());
            new PrimaryOperation<Request, ReplicaRequest, Response>(request, checkWriteConsistency, logger, shard,
                    clusterService::state, actionName) {
                @Override
                protected void finishedAsSuccess(Response response) {
                    setPhase(replicationTask, "finished");
                    shard.close();
                    try {
                        channel.sendResponse(response);
                    } catch (IOException responseException) {
                        logger.warn("failed to send response message back to client for action [" + transportPrimaryAction + "]",
                                responseException);
                    }
                }

                @Override
                protected void finishedAsFailed(Throwable failure) {
                    setPhase(replicationTask, "failed");
                    shard.close();
                    try {
                        channel.sendResponse(failure);
                    } catch (IOException responseException) {
                        logger.warn("failed to send failure message back to client for action [" + transportPrimaryAction + "]",
                                responseException);
                    }
                }

                @Override
                protected void routeToRelocatedPrimary(DiscoveryNode targetNode, ActionListener<Response> callback) {
                    transportService.sendRequest(targetNode, transportPrimaryAction, request, transportOptions,
                            ActionListenerResponseHandler.responseHandler(callback, TransportReplicationAction2.this::newResponseInstance));
                }

                @Override
                protected void performOnReplica(DiscoveryNode node, ShardRouting shard, ReplicaRequest request, ActionListener<Void>
                        callback) {
                    transportService.sendRequest(node, transportReplicaAction, request, transportOptions, new
                            EmptyTransportResponseHandler(ThreadPool.Names.SAME) {
                                @Override
                                public void handleResponse(TransportResponse.Empty vResponse) {
                                    callback.onResponse(null);
                                }

                                @Override
                                public void handleException(TransportException exp) {
                                    callback.onFailure(exp);
                                }
                            }
                    );
                }

                @Override
                protected void failReplicaOnMaster(ShardRouting shard, ShardRouting primaryRouting, String message, Throwable exception,
                                                   ShardStateAction.Listener callback) {
                    logger.warn("[{}] {}", exception, shard.shardId(), message);
                    shardStateAction.shardFailed(shard, primaryRouting, message, exception, callback);
                }

                @Override
                protected void moveToReplication(ReplicationPhase replicationPhase) {
                    setPhase(replicationTask, "replicating");
                    super.moveToReplication(replicationPhase);
                }
            }.run();
        }
    }

    class ReplicaOperationTransportHandler implements TransportRequestHandler<ReplicaRequest> {
        @Override
        public void messageReceived(final ReplicaRequest request, final TransportChannel channel) throws Exception {
            throw new UnsupportedOperationException("the task parameter is required for this operation");
        }

        @Override
        public void messageReceived(ReplicaRequest request, TransportChannel channel, Task task) throws Exception {
            final ReplicationTask replicationTask = (ReplicationTask) task;
            setPhase(replicationTask, "replica");
            final ClusterStateObserver observer = new ClusterStateObserver(clusterService, request.timeout(), logger,
                    threadPool.getThreadContext());
            final IndexShardReference<Request, ReplicaRequest, Response> shard = getIndexShardReferenceOnReplica(request.shardId());
            new ReplicaOperation<Request, ReplicaRequest, Response>(request, shard, observer, logger, transportReplicaAction, threadPool
                    .executor(executor)::execute) {
                @Override
                protected void finishAsFailed(Throwable failure) {
                    setPhase(replicationTask, "failed");
                    shard.close();
                    try {
                        channel.sendResponse(failure);
                    } catch (IOException responseException) {
                        logger.warn("failed to send failure message back to client for action [" + transportReplicaAction + "]",
                                responseException);
                    }
                }

                @Override
                protected void finishAsSuccessful() {
                    setPhase(replicationTask, "finished");
                    shard.close();
                    try {
                        channel.sendResponse(TransportResponse.Empty.INSTANCE);
                    } catch (IOException responseException) {
                        logger.warn("failed to send response message back to client for action [" + transportPrimaryAction + "]",
                                responseException);
                    }
                }
            }.run();
        }
    }

    /**
     * returns a new reference to {@link IndexShard} to perform a primary operation. Released after performing primary operation locally
     * and replication of the operation to all replica shards is completed / failed (see {@link PrimaryOperation.ReplicationPhase}).
     */
    protected IndexShardReference<Request, ReplicaRequest, Response> getIndexShardReferenceOnPrimary(ShardId shardId) {
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        IndexShard indexShard = indexService.getShard(shardId.id());
        Releasable operationLock = null;
        try {
            operationLock = indexShard.acquirePrimaryOperationLock();
            IndexShardReference ref = createShardReference(indexShard, operationLock);
            operationLock = null;
            return ref;
        } finally {
            if (operationLock != null) {
                operationLock.close();
            }
        }
    }

    /**
     * returns a new reference to {@link IndexShard} on a node that the request is replicated to. The reference is closed as soon as
     * replication is completed on the node.
     */
    protected IndexShardReference<Request, ReplicaRequest, Response> getIndexShardReferenceOnReplica(ShardId shardId) {
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        IndexShard indexShard = indexService.getShard(shardId.id());
        Releasable operationLock = null;
        try {
            operationLock = indexShard.acquireReplicaOperationLock();
            IndexShardReference ref = createShardReference(indexShard, operationLock);
            operationLock = null;
            return ref;
        } finally {
            if (operationLock != null) {
                operationLock.close();
            }
        }
    }

    abstract protected IndexShardReferenceBase createShardReference(IndexShard shard, Releasable opeationLock);


    protected abstract class IndexShardReferenceBase implements IndexShardReference<Request, ReplicaRequest, Response> {

        private final IndexShard indexShard;
        private final Releasable operationLock;

        protected IndexShardReferenceBase(IndexShard indexShard, Releasable operationLock) {
            this.indexShard = indexShard;
            this.operationLock = operationLock;
        }

        @Override
        public void close() {
            operationLock.close();
        }

        @Override
        public boolean isRelocated() {
            return indexShard.state() == IndexShardState.RELOCATED;
        }

        @Override
        public ShardRouting routingEntry() {
            return indexShard.routingEntry();
        }

        @Override
        public void failShard(String reason, Throwable t) {
            indexShard.failShard(reason, t);
        }
    }

    protected final void processAfterWrite(boolean refresh, IndexShard indexShard, Translog.Location location) {
        if (refresh) {
            try {
                indexShard.refresh("refresh_flag_index");
            } catch (Throwable e) {
                // ignore
            }
        }
        if (indexShard.getTranslogDurability() == Translog.Durability.REQUEST && location != null) {
            indexShard.sync(location);
        }
        indexShard.maybeFlush();
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
