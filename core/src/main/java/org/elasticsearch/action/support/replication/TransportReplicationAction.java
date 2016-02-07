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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ReplicationResponse;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportResponseHandler;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.EmptyTransportResponseHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportChannelResponseHandler;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Base class for requests that should be executed on a primary copy followed by replica copies.
 * Subclasses can resolve the target shard and provide implementation for primary and replica operations.
 *
 * The action samples cluster state on the receiving node to reroute to node with primary copy and on the
 * primary node to validate request before primary operation followed by sampling state again for resolving
 * nodes with replica copies to perform replication.
 */
public abstract class TransportReplicationAction<Request extends ReplicationRequest<Request>, ReplicaRequest extends ReplicationRequest<ReplicaRequest>, Response extends ReplicationResponse> extends TransportAction<Request, Response> {

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

    protected TransportReplicationAction(Settings settings, String actionName, TransportService transportService,
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
        transportService.registerRequestHandler(transportReplicaAction, replicaRequest, executor, true, new ReplicaOperationTransportHandler());

        this.transportOptions = transportOptions();

        this.defaultWriteConsistencyLevel = WriteConsistencyLevel.fromString(settings.get("action.write_consistency", "quorum"));
    }

    @Override
    protected final void doExecute(Request request, ActionListener<Response> listener) {
        throw new UnsupportedOperationException("the task parameter is required for this operation");
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        new ReroutePhase((ReplicationTask) task, request, listener).run();
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
     * Primary operation on node with primary copy, the provided metadata should be used for request validation if needed
     *
     * @return A tuple containing not null values, as first value the result of the primary operation and as second value
     * the request to be executed on the replica shards.
     */
    protected abstract Tuple<Response, ReplicaRequest> shardOperationOnPrimary(MetaData metaData, Request shardRequest) throws Exception;

    /**
     * Replica operation on nodes with replica copies
     */
    protected abstract void shardOperationOnReplica(ReplicaRequest shardRequest);

    /**
     * True if write consistency should be checked for an implementation
     */
    protected boolean checkWriteConsistency() {
        return true;
    }

    /**
     * Cluster level block to check before request execution
     */
    protected ClusterBlockLevel globalBlockLevel() {
        return ClusterBlockLevel.WRITE;
    }

    /**
     * Index level block to check before request execution
     */
    protected ClusterBlockLevel indexBlockLevel() {
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

    protected boolean retryPrimaryException(Throwable e) {
        return e.getClass() == RetryOnPrimaryException.class
                || TransportActions.isShardNotAvailableException(e);
    }

    /**
     * Should an exception be ignored when the operation is performed on the replica.
     */
    protected boolean ignoreReplicaException(Throwable e) {
        if (TransportActions.isShardNotAvailableException(e)) {
            return true;
        }
        // on version conflict or document missing, it means
        // that a new change has crept into the replica, and it's fine
        if (isConflictException(e)) {
            return true;
        }
        return false;
    }

    protected boolean isConflictException(Throwable e) {
        Throwable cause = ExceptionsHelper.unwrapCause(e);
        // on version conflict or document missing, it means
        // that a new change has crept into the replica, and it's fine
        if (cause instanceof VersionConflictEngineException) {
            return true;
        }
        return false;
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
            new PrimaryPhase((ReplicationTask) task, request, channel).run();
        }
    }

    class ReplicaOperationTransportHandler implements TransportRequestHandler<ReplicaRequest> {
        @Override
        public void messageReceived(final ReplicaRequest request, final TransportChannel channel) throws Exception {
            throw new UnsupportedOperationException("the task parameter is required for this operation");
        }

        @Override
        public void messageReceived(ReplicaRequest request, TransportChannel channel, Task task) throws Exception {
            new AsyncReplicaAction(request, channel, (ReplicationTask) task).run();
        }
    }

    public static class RetryOnReplicaException extends ElasticsearchException {

        public RetryOnReplicaException(ShardId shardId, String msg) {
            super(msg);
            setShard(shardId);
        }

        public RetryOnReplicaException(StreamInput in) throws IOException {
            super(in);
        }
    }

    private final class AsyncReplicaAction extends AbstractRunnable {
        private final ReplicaRequest request;
        private final TransportChannel channel;
        /**
         * The task on the node with the replica shard.
         */
        private final ReplicationTask task;
        // important: we pass null as a timeout as failing a replica is
        // something we want to avoid at all costs
        private final ClusterStateObserver observer = new ClusterStateObserver(clusterService, null, logger, threadPool.getThreadContext());

        AsyncReplicaAction(ReplicaRequest request, TransportChannel channel, ReplicationTask task) {
            this.request = request;
            this.channel = channel;
            this.task = task;
        }

        @Override
        public void onFailure(Throwable t) {
            if (t instanceof RetryOnReplicaException) {
                logger.trace("Retrying operation on replica, action [{}], request [{}]", t, transportReplicaAction, request);
                final ThreadContext.StoredContext context = threadPool.getThreadContext().newStoredContext();
                observer.waitForNextChange(new ClusterStateObserver.Listener() {
                    @Override
                    public void onNewClusterState(ClusterState state) {
                        context.close();
                        // Forking a thread on local node via transport service so that custom transport service have an
                        // opportunity to execute custom logic before the replica operation begins
                        String extraMessage = "action [" + transportReplicaAction + "], request[" + request + "]";
                        TransportChannelResponseHandler<TransportResponse.Empty> handler = TransportChannelResponseHandler.emptyResponseHandler(logger, channel, extraMessage);
                        transportService.sendRequest(clusterService.localNode(), transportReplicaAction, request, handler);
                    }

                    @Override
                    public void onClusterServiceClose() {
                        responseWithFailure(new NodeClosedException(clusterService.localNode()));
                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        throw new AssertionError("Cannot happen: there is not timeout");
                    }
                });
            } else {
                try {
                    failReplicaIfNeeded(t);
                } catch (Throwable unexpected) {
                    logger.error("{} unexpected error while failing replica", unexpected, request.shardId().id());
                } finally {
                    responseWithFailure(t);
                }
            }
        }

        private void failReplicaIfNeeded(Throwable t) {
            String index = request.shardId().getIndex().getName();
            int shardId = request.shardId().id();
            logger.trace("failure on replica [{}][{}], action [{}], request [{}]", t, index, shardId, actionName, request);
            if (ignoreReplicaException(t) == false) {
                IndexService indexService = indicesService.indexService(index);
                if (indexService == null) {
                    logger.debug("ignoring failed replica [{}][{}] because index was already removed.", index, shardId);
                    return;
                }
                IndexShard indexShard = indexService.getShardOrNull(shardId);
                if (indexShard == null) {
                    logger.debug("ignoring failed replica [{}][{}] because index was already removed.", index, shardId);
                    return;
                }
                indexShard.failShard(actionName + " failed on replica", t);
            }
        }

        protected void responseWithFailure(Throwable t) {
            try {
                channel.sendResponse(t);
            } catch (IOException responseException) {
                logger.warn("failed to send error message back to client for action [" + transportReplicaAction + "]", responseException);
                logger.warn("actual Exception", t);
            }
        }

        @Override
        protected void doRun() throws Exception {
            setPhase(task, "replica");
            assert request.shardId() != null : "request shardId must be set";
            try (Releasable ignored = getIndexShardReferenceOnReplica(request.shardId())) {
                shardOperationOnReplica(request);
                if (logger.isTraceEnabled()) {
                    logger.trace("action [{}] completed on shard [{}] for request [{}]", transportReplicaAction, request.shardId(), request);
                }
            }
            setPhase(task, "finished");
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }
    }

    public static class RetryOnPrimaryException extends ElasticsearchException {
        public RetryOnPrimaryException(ShardId shardId, String msg) {
            super(msg);
            setShard(shardId);
        }

        public RetryOnPrimaryException(StreamInput in) throws IOException {
            super(in);
        }
    }

    /**
     * Responsible for routing and retrying failed operations on the primary.
     * The actual primary operation is done in {@link PrimaryPhase} on the
     * node with primary copy.
     *
     * Resolves index and shard id for the request before routing it to target node
     */
    final class ReroutePhase extends AbstractRunnable {
        private final ActionListener<Response> listener;
        private final Request request;
        private final ReplicationTask task;
        private final ClusterStateObserver observer;
        private final AtomicBoolean finished = new AtomicBoolean();

        ReroutePhase(ReplicationTask task, Request request, ActionListener<Response> listener) {
            this.request = request;
            if (task != null) {
                this.request.setParentTask(clusterService.localNode().getId(), task.getId());
            }
            this.listener = listener;
            this.task = task;
            this.observer = new ClusterStateObserver(clusterService, request.timeout(), logger, threadPool.getThreadContext());
        }

        @Override
        public void onFailure(Throwable e) {
            finishWithUnexpectedFailure(e);
        }

        @Override
        protected void doRun() {
            setPhase(task, "routing");
            final ClusterState state = observer.observedState();
            ClusterBlockException blockException = state.blocks().globalBlockedException(globalBlockLevel());
            if (blockException != null) {
                handleBlockException(blockException);
                return;
            }
            final String concreteIndex = resolveIndex() ? indexNameExpressionResolver.concreteSingleIndex(state, request) : request.index();
            blockException = state.blocks().indexBlockedException(indexBlockLevel(), concreteIndex);
            if (blockException != null) {
                handleBlockException(blockException);
                return;
            }

            // request does not have a shardId yet, we need to pass the concrete index to resolve shardId
            resolveRequest(state.metaData(), concreteIndex, request);
            assert request.shardId() != null : "request shardId must be set in resolveRequest";

            IndexShardRoutingTable indexShard = state.getRoutingTable().shardRoutingTable(request.shardId());
            final ShardRouting primary = indexShard.primaryShard();
            if (primary == null || primary.active() == false) {
                logger.trace("primary shard [{}] is not yet active, scheduling a retry: action [{}], request [{}], cluster state version " +
                        "[{}]", request.shardId(), actionName, request, state.version());
                retryBecauseUnavailable(request.shardId(), "primary shard is not active");
                return;
            }
            if (state.nodes().nodeExists(primary.currentNodeId()) == false) {
                logger.trace("primary shard [{}] is assigned to an unknown node [{}], scheduling a retry: action [{}], request [{}], " +
                        "cluster state version [{}]", request.shardId(), primary.currentNodeId(), actionName, request, state.version());
                retryBecauseUnavailable(request.shardId(), "primary shard isn't assigned to a known node.");
                return;
            }
            final DiscoveryNode node = state.nodes().get(primary.currentNodeId());
            if (primary.currentNodeId().equals(state.nodes().localNodeId())) {
                setPhase(task, "waiting_on_primary");
                if (logger.isTraceEnabled()) {
                    logger.trace("send action [{}] on primary [{}] for request [{}] with cluster state version [{}] to [{}] ",
                            transportPrimaryAction, request.shardId(), request, state.version(), primary.currentNodeId());
                }
                performAction(node, transportPrimaryAction, true);
            } else {
                if (state.version() < request.routedBasedOnClusterVersion()) {
                    logger.trace("failed to find primary [{}] for request [{}] despite sender thinking it would be here. Local cluster " +
                            "state version [{}]] is older than on sending node (version [{}]), scheduling a retry...", request.shardId(),
                            request, state.version(), request.routedBasedOnClusterVersion());
                    retryBecauseUnavailable(request.shardId(), "failed to find primary as current cluster state with version [" + state
                            .version() + "] is stale (expected at least [" + request.routedBasedOnClusterVersion() + "]");
                    return;
                } else {
                    // chasing the node with the active primary for a second hop requires that we are at least up-to-date with the
                    // current cluster state version
                    // this prevents redirect loops between two nodes when a primary was relocated and the relocation target is not aware
                    // that it is the active primary shard already.
                    request.routedBasedOnClusterVersion(state.version());
                }
                if (logger.isTraceEnabled()) {
                    logger.trace("send action [{}] on primary [{}] for request [{}] with cluster state version [{}] to [{}]", actionName,
                            request.shardId(), request, state.version(), primary.currentNodeId());
                }
                setPhase(task, "rerouted");
                performAction(node, actionName, false);
            }
        }

        private void handleBlockException(ClusterBlockException blockException) {
            if (blockException.retryable()) {
                logger.trace("cluster is blocked ({}), scheduling a retry", blockException.getMessage());
                retry(blockException);
            } else {
                finishAsFailed(blockException);
            }
        }

        private void performAction(final DiscoveryNode node, final String action, final boolean isPrimaryAction) {
            transportService.sendRequest(node, action, request, transportOptions, new BaseTransportResponseHandler<Response>() {

                @Override
                public Response newInstance() {
                    return newResponseInstance();
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }

                @Override
                public void handleResponse(Response response) {
                    finishOnSuccess(response);
                }

                @Override
                public void handleException(TransportException exp) {
                    try {
                        // if we got disconnected from the node, or the node / shard is not in the right state (being closed)
                        if (exp.unwrapCause() instanceof ConnectTransportException || exp.unwrapCause() instanceof NodeClosedException ||
                                (isPrimaryAction && retryPrimaryException(exp.unwrapCause()))) {
                            logger.trace("received an error from node [{}] for request [{}], scheduling a retry", exp, node.id(), request);
                            retry(exp);
                        } else {
                            finishAsFailed(exp);
                        }
                    } catch (Throwable t) {
                        finishWithUnexpectedFailure(t);
                    }
                }
            });
        }

        void retry(Throwable failure) {
            assert failure != null;
            if (observer.isTimedOut()) {
                // we running as a last attempt after a timeout has happened. don't retry
                finishAsFailed(failure);
                return;
            }
            setPhase(task, "waiting_for_retry");
            final ThreadContext.StoredContext context = threadPool.getThreadContext().newStoredContext();
            observer.waitForNextChange(new ClusterStateObserver.Listener() {
                @Override
                public void onNewClusterState(ClusterState state) {
                    context.close();
                    run();
                }

                @Override
                public void onClusterServiceClose() {
                    finishAsFailed(new NodeClosedException(clusterService.localNode()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    context.close();
                    // Try one more time...
                    run();
                }
            });
        }

        void finishAsFailed(Throwable failure) {
            if (finished.compareAndSet(false, true)) {
                setPhase(task, "failed");
                logger.trace("operation failed. action [{}], request [{}]", failure, actionName, request);
                listener.onFailure(failure);
            } else {
                assert false : "finishAsFailed called but operation is already finished";
            }
        }

        void finishWithUnexpectedFailure(Throwable failure) {
            logger.warn("unexpected error during the primary phase for action [{}], request [{}]", failure, actionName, request);
            if (finished.compareAndSet(false, true)) {
                setPhase(task, "failed");
                listener.onFailure(failure);
            } else {
                assert false : "finishWithUnexpectedFailure called but operation is already finished";
            }
        }

        void finishOnSuccess(Response response) {
            if (finished.compareAndSet(false, true)) {
                setPhase(task, "finished");
                if (logger.isTraceEnabled()) {
                    logger.trace("operation succeeded. action [{}],request [{}]", actionName, request);
                }
                listener.onResponse(response);
            } else {
                assert false : "finishOnSuccess called but operation is already finished";
            }
        }

        void retryBecauseUnavailable(ShardId shardId, String message) {
            retry(new UnavailableShardsException(shardId, "{} Timeout: [{}], request: [{}]", message, request.timeout(), request));
        }
    }

    /**
     * Responsible for performing primary operation locally or delegating primary operation to relocation target in case where shard has
     * been marked as RELOCATED. Delegates to replication action once successful.
     * <p>
     * Note that as soon as we move to replication action, state responsibility is transferred to {@link ReplicationPhase}.
     */
    class PrimaryPhase extends AbstractRunnable {
        private final ReplicationTask task;
        private final Request request;
        private final ShardId shardId;
        private final TransportChannel channel;
        private final ClusterState state;
        private final AtomicBoolean finished = new AtomicBoolean();
        private IndexShardReference indexShardReference;

        PrimaryPhase(ReplicationTask task, Request request, TransportChannel channel) {
            this.state = clusterService.state();
            this.task = task;
            this.request = request;
            assert request.shardId() != null : "request shardId must be set prior to primary phase";
            this.shardId = request.shardId();
            this.channel = channel;
        }

        @Override
        public void onFailure(Throwable e) {
            if (ExceptionsHelper.status(e) == RestStatus.CONFLICT) {
                if (logger.isTraceEnabled()) {
                    logger.trace("failed to execute [{}] on [{}]", e, request, shardId);
                }
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("failed to execute [{}] on [{}]", e, request, shardId);
                }
            }
            finishAsFailed(e);
        }

        @Override
        protected void doRun() throws Exception {
            setPhase(task, "primary");
            // request shardID was set in ReroutePhase
            final String writeConsistencyFailure = checkWriteConsistency(shardId);
            if (writeConsistencyFailure != null) {
                finishBecauseUnavailable(shardId, writeConsistencyFailure);
                return;
            }
            // closed in finishAsFailed(e) in the case of error
            indexShardReference = getIndexShardReferenceOnPrimary(shardId);
            if (indexShardReference.isRelocated() == false) {
                // execute locally
                Tuple<Response, ReplicaRequest> primaryResponse = shardOperationOnPrimary(state.metaData(), request);
                if (logger.isTraceEnabled()) {
                    logger.trace("action [{}] completed on shard [{}] for request [{}] with cluster state version [{}]", transportPrimaryAction, shardId, request, state.version());
                }
                ReplicationPhase replicationPhase = new ReplicationPhase(task, primaryResponse.v2(), primaryResponse.v1(), shardId, channel, indexShardReference);
                finishAndMoveToReplication(replicationPhase);
            } else {
                // delegate primary phase to relocation target
                // it is safe to execute primary phase on relocation target as there are no more in-flight operations where primary
                // phase is executed on local shard and all subsequent operations are executed on relocation target as primary phase.
                final ShardRouting primary = indexShardReference.routingEntry();
                indexShardReference.close();
                assert primary.relocating() : "indexShard is marked as relocated but routing isn't" + primary;
                DiscoveryNode relocatingNode = state.nodes().get(primary.relocatingNodeId());
                transportService.sendRequest(relocatingNode, transportPrimaryAction, request, transportOptions,
                        TransportChannelResponseHandler.responseHandler(logger, TransportReplicationAction.this::newResponseInstance, channel,
                                "rerouting indexing to target primary " + primary));
            }
        }

        /**
         * checks whether we can perform a write based on the write consistency setting
         * returns **null* if OK to proceed, or a string describing the reason to stop
         */
        String checkWriteConsistency(ShardId shardId) {
            if (checkWriteConsistency == false) {
                return null;
            }

            final WriteConsistencyLevel consistencyLevel;
            if (request.consistencyLevel() != WriteConsistencyLevel.DEFAULT) {
                consistencyLevel = request.consistencyLevel();
            } else {
                consistencyLevel = defaultWriteConsistencyLevel;
            }
            final int sizeActive;
            final int requiredNumber;
            IndexRoutingTable indexRoutingTable = state.getRoutingTable().index(shardId.getIndexName());
            if (indexRoutingTable != null) {
                IndexShardRoutingTable shardRoutingTable = indexRoutingTable.shard(shardId.getId());
                if (shardRoutingTable != null) {
                    sizeActive = shardRoutingTable.activeShards().size();
                    if (consistencyLevel == WriteConsistencyLevel.QUORUM && shardRoutingTable.getSize() > 2) {
                        // only for more than 2 in the number of shardIt it makes sense, otherwise its 1 shard with 1 replica, quorum is 1 (which is what it is initialized to)
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
                logger.trace("not enough active copies of shard [{}] to meet write consistency of [{}] (have {}, needed {}), scheduling a retry. action [{}], request [{}]",
                        shardId, consistencyLevel, sizeActive, requiredNumber, transportPrimaryAction, request);
                return "Not enough active copies to meet write consistency of [" + consistencyLevel + "] (have " + sizeActive + ", needed " + requiredNumber + ").";
            } else {
                return null;
            }
        }

        /**
         * upon success, finish the first phase and transfer responsibility to the {@link ReplicationPhase}
         */
        void finishAndMoveToReplication(ReplicationPhase replicationPhase) {
            if (finished.compareAndSet(false, true)) {
                replicationPhase.run();
            } else {
                assert false : "finishAndMoveToReplication called but operation is already finished";
            }
        }

        /**
         * upon failure, send failure back to the {@link ReroutePhase} for retrying if appropriate
         */
        void finishAsFailed(Throwable failure) {
            if (finished.compareAndSet(false, true)) {
                setPhase(task, "failed");
                Releasables.close(indexShardReference);
                logger.trace("operation failed", failure);
                try {
                    channel.sendResponse(failure);
                } catch (IOException responseException) {
                    logger.warn("failed to send error message back to client for action [{}]", responseException, transportPrimaryAction);
                }
            } else {
                assert false : "finishAsFailed called but operation is already finished";
            }
        }

        void finishBecauseUnavailable(ShardId shardId, String message) {
            finishAsFailed(new UnavailableShardsException(shardId, "{} Timeout: [{}], request: [{}]", message, request.timeout(), request));
        }
    }

    /**
     * returns a new reference to {@link IndexShard} to perform a primary operation. Released after performing primary operation locally
     * and replication of the operation to all replica shards is completed / failed (see {@link ReplicationPhase}).
     */
    protected IndexShardReference getIndexShardReferenceOnPrimary(ShardId shardId) {
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        IndexShard indexShard = indexService.getShard(shardId.id());
        return new IndexShardReferenceImpl(indexShard, true);
    }

    /**
     * returns a new reference to {@link IndexShard} on a node that the request is replicated to. The reference is closed as soon as
     * replication is completed on the node.
     */
    protected IndexShardReference getIndexShardReferenceOnReplica(ShardId shardId) {
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        IndexShard indexShard = indexService.getShard(shardId.id());
        return new IndexShardReferenceImpl(indexShard, false);
    }

    /**
     * Responsible for sending replica requests (see {@link AsyncReplicaAction}) to nodes with replica copy, including
     * relocating copies
     */
    final class ReplicationPhase extends AbstractRunnable {
        private final ReplicationTask task;
        private final ReplicaRequest replicaRequest;
        private final Response finalResponse;
        private final TransportChannel channel;
        private final ShardId shardId;
        private final List<ShardRouting> shards;
        private final DiscoveryNodes nodes;
        private final boolean executeOnReplica;
        private final AtomicBoolean finished = new AtomicBoolean();
        private final AtomicInteger success = new AtomicInteger(1); // We already wrote into the primary shard
        private final ConcurrentMap<String, Throwable> shardReplicaFailures = ConcurrentCollections.newConcurrentMap();
        private final AtomicInteger pending;
        private final int totalShards;
        private final IndexShardReference indexShardReference;

        public ReplicationPhase(ReplicationTask task, ReplicaRequest replicaRequest, Response finalResponse, ShardId shardId,
                                TransportChannel channel, IndexShardReference indexShardReference) {
            this.task = task;
            this.replicaRequest = replicaRequest;
            this.channel = channel;
            this.finalResponse = finalResponse;
            this.indexShardReference = indexShardReference;
            this.shardId = shardId;

            // we have to get a new state after successfully indexing into the primary in order to honour recovery semantics.
            // we have to make sure that every operation indexed into the primary after recovery start will also be replicated
            // to the recovery target. If we use an old cluster state, we may miss a relocation that has started since then.
            // If the index gets deleted after primary operation, we skip replication
            final ClusterState state = clusterService.state();
            final IndexRoutingTable index = state.getRoutingTable().index(shardId.getIndex());
            final IndexShardRoutingTable shardRoutingTable = (index != null) ? index.shard(shardId.id()) : null;
            final IndexMetaData indexMetaData = state.getMetaData().index(shardId.getIndex());
            this.shards = (shardRoutingTable != null) ? shardRoutingTable.shards() : Collections.emptyList();
            this.executeOnReplica = (indexMetaData == null) || shouldExecuteReplication(indexMetaData.getSettings());
            this.nodes = state.getNodes();

            if (shards.isEmpty()) {
                logger.debug("replication phase for request [{}] on [{}] is skipped due to index deletion after primary operation", replicaRequest, shardId);
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
                logger.trace("replication phase started. pending [{}], action [{}], request [{}], cluster state version used [{}]", pending.get(),
                        transportReplicaAction, replicaRequest, state.version());
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

        @Override
        public void onFailure(Throwable t) {
            logger.error("unexpected error while replicating for action [{}]. shard [{}]. ", t, actionName, shardId);
            forceFinishAsFailed(t);
        }

        /**
         * start sending replica requests to target nodes
         */
        @Override
        protected void doRun() {
            setPhase(task, "replicating");
            if (pending.get() == 0) {
                doFinish();
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
                    performOnReplica(shard);
                }
                // send operation to relocating shard
                // local shard can be a relocation target of a primary that is in relocated state
                if (shard.relocating() && nodes.localNodeId().equals(shard.relocatingNodeId()) == false) {
                    performOnReplica(shard.buildTargetRelocatingShard());
                }
            }
        }

        /**
         * send replica operation to target node
         */
        void performOnReplica(final ShardRouting shard) {
            // if we don't have that node, it means that it might have failed and will be created again, in
            // this case, we don't have to do the operation, and just let it failover
            String nodeId = shard.currentNodeId();
            if (!nodes.nodeExists(nodeId)) {
                logger.trace("failed to send action [{}] on replica [{}] for request [{}] due to unknown node [{}]", transportReplicaAction, shard.shardId(), replicaRequest, nodeId);
                onReplicaFailure(nodeId, null);
                return;
            }
            if (logger.isTraceEnabled()) {
                logger.trace("send action [{}] on replica [{}] for request [{}] to [{}]", transportReplicaAction, shard.shardId(), replicaRequest, nodeId);
            }

            final DiscoveryNode node = nodes.get(nodeId);
            transportService.sendRequest(node, transportReplicaAction, replicaRequest, transportOptions, new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {
                        @Override
                        public void handleResponse(TransportResponse.Empty vResponse) {
                            onReplicaSuccess();
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            logger.trace("[{}] transport failure during replica request [{}], action [{}]", exp, node, replicaRequest, transportReplicaAction);
                            if (ignoreReplicaException(exp)) {
                                onReplicaFailure(nodeId, exp);
                            } else {
                                String message = String.format(Locale.ROOT, "failed to perform %s on replica on node %s", transportReplicaAction, node);
                                logger.warn("[{}] {}", exp, shardId, message);
                                shardStateAction.shardFailed(
                                    shard,
                                    indexShardReference.routingEntry(),
                                    message,
                                    exp,
                                    new ShardStateAction.Listener() {
                                        @Override
                                        public void onSuccess() {
                                            onReplicaFailure(nodeId, exp);
                                        }

                                        @Override
                                        public void onFailure(Throwable t) {
                                            // TODO: handle catastrophic non-channel failures
                                            onReplicaFailure(nodeId, exp);
                                        }
                                    }
                                );
                            }
                        }
                    }
            );
        }

        void onReplicaFailure(String nodeId, @Nullable Throwable e) {
            // Only version conflict should be ignored from being put into the _shards header?
            if (e != null && ignoreReplicaException(e) == false) {
                shardReplicaFailures.put(nodeId, e);
            }
            decPendingAndFinishIfNeeded();
        }

        void onReplicaSuccess() {
            success.incrementAndGet();
            decPendingAndFinishIfNeeded();
        }

        private void decPendingAndFinishIfNeeded() {
            if (pending.decrementAndGet() <= 0) {
                doFinish();
            }
        }

        private void forceFinishAsFailed(Throwable t) {
            setPhase(task, "failed");
            if (finished.compareAndSet(false, true)) {
                Releasables.close(indexShardReference);
                try {
                    channel.sendResponse(t);
                } catch (IOException responseException) {
                    logger.warn("failed to send error message back to client for action [{}]", responseException, transportReplicaAction);
                    logger.warn("actual Exception", t);
                }
            }
        }

        private void doFinish() {
            if (finished.compareAndSet(false, true)) {
                setPhase(task, "finished");
                Releasables.close(indexShardReference);
                final ReplicationResponse.ShardInfo.Failure[] failuresArray;
                if (!shardReplicaFailures.isEmpty()) {
                    int slot = 0;
                    failuresArray = new ReplicationResponse.ShardInfo.Failure[shardReplicaFailures.size()];
                    for (Map.Entry<String, Throwable> entry : shardReplicaFailures.entrySet()) {
                        RestStatus restStatus = ExceptionsHelper.status(entry.getValue());
                        failuresArray[slot++] = new ReplicationResponse.ShardInfo.Failure(shardId, entry.getKey(), entry.getValue(), restStatus, false);
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
                try {
                    channel.sendResponse(finalResponse);
                } catch (IOException responseException) {
                    logger.warn("failed to send error message back to client for action [" + transportReplicaAction + "]", responseException);
                }
                if (logger.isTraceEnabled()) {
                    logger.trace("action [{}] completed on all replicas [{}] for request [{}]", transportReplicaAction, shardId, replicaRequest);
                }
            }
        }
    }

    /**
     * Indicated whether this operation should be replicated to shadow replicas or not. If this method returns true the replication phase will be skipped.
     * For example writes such as index and delete don't need to be replicated on shadow replicas but refresh and flush do.
     */
    protected boolean shouldExecuteReplication(Settings settings) {
        return IndexMetaData.isIndexUsingShadowReplicas(settings) == false;
    }

    interface IndexShardReference extends Releasable {
        boolean isRelocated();

        ShardRouting routingEntry();
    }

    static final class IndexShardReferenceImpl implements IndexShardReference {

        private final IndexShard indexShard;
        private final Releasable operationLock;

        IndexShardReferenceImpl(IndexShard indexShard, boolean primaryAction) {
            this.indexShard = indexShard;
            if (primaryAction) {
                operationLock = indexShard.acquirePrimaryOperationLock();
            } else {
                operationLock = indexShard.acquireReplicaOperationLock();
            }
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
