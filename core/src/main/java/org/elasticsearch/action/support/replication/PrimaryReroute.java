package org.elasticsearch.action.support.replication;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ReplicationResponse;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.transport.ConnectTransportException;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class PrimaryReroute<Request extends ReplicationRequest<Request>, Response extends ReplicationResponse> extends
        AbstractRunnable {
    private final Request request;
    private final ClusterStateObserver observer;
    private final ClusterBlockLevel blockLevel;
    private final String opType;
    private final ESLogger logger;
    private final AtomicBoolean finished = new AtomicBoolean();
    private final ActionListener<Response> listener;
    private final NodeAction<Request, Response> primaryAction;
    private final NodeAction<Request, Response> rerouteAction;

    PrimaryReroute(Request request, ClusterStateObserver observer, ClusterBlockLevel blockLevel, String opType, ESLogger logger, ActionListener<Response> listener,
                   NodeAction<Request, Response> primaryAction, NodeAction<Request, Response> rerouteAction) {
        this.request = request;
        this.observer = observer;
        this.blockLevel = blockLevel;
        this.opType = opType;
        this.logger = logger;
        this.listener = listener;
        this.primaryAction = primaryAction;
        this.rerouteAction = rerouteAction;
    }

    @Override
    public void onFailure(Throwable e) {
        finishWithUnexpectedFailure(e);
    }

    @Override
    protected void doRun() {
        final ClusterState state = observer.observedState();
        ClusterBlockException blockException = state.blocks().globalBlockedException(blockLevel);
        if (blockException != null) {
            handleBlockException(blockException);
            return;
        }
        blockException = state.blocks().indexBlockedException(blockLevel, request.shardId().getIndexName());
        if (blockException != null) {
            handleBlockException(blockException);
            return;
        }


        IndexShardRoutingTable indexShard = state.getRoutingTable().shardRoutingTable(request.shardId());
        final ShardRouting primary = indexShard.primaryShard();
        if (primary == null || primary.active() == false) {
            logger.trace("primary shard [{}] is not yet active, scheduling a retry: action [{}], request [{}], cluster state version [{}]",
                    request.shardId(), opType, request, state.version());
            retryBecauseUnavailable(request.shardId(), "primary shard is not active");
            return;
        }
        if (state.nodes().nodeExists(primary.currentNodeId()) == false) {
            logger.trace("primary shard [{}] is assigned to an unknown node [{}], scheduling a retry: action [{}], request [{}], cluster " +
                    "state version [{}]",
                    request.shardId(), primary.currentNodeId(), opType, request, state.version());
            retryBecauseUnavailable(request.shardId(), "primary shard isn't assigned to a known node.");
            return;
        }
        final DiscoveryNode node = state.nodes().get(primary.currentNodeId());
        if (primary.currentNodeId().equals(state.nodes().localNodeId())) {
            if (logger.isTraceEnabled()) {
                logger.trace("send action [{}] on primary [{}] for request [{}] with cluster state version [{}] to [{}] ", opType,
                        request.shardId(), request, state.version(), primary.currentNodeId());
            }
            primaryAction.execute(node, request, ActionListener.wrap(this::finishOnSuccess, this::handleErrorResponse));
        } else {
            if (state.version() < request.routedBasedOnClusterVersion()) {
                logger.trace("failed to find primary [{}] for request [{}] despite sender thinking it would be here. Local cluster state " +
                        "version [{}]] is older than on sending node (version [{}]), scheduling a retry...", request.shardId(), request,
                        state.version(), request.routedBasedOnClusterVersion());
                retryBecauseUnavailable(request.shardId(), "failed to find primary as current cluster state with version [" + state
                        .version() + "] is stale (expected at least [" + request.routedBasedOnClusterVersion() + "]");
                return;
            } else {
                // chasing the node with the active primary for a second hop requires that we are at least up-to-date with the current
                // cluster state version
                // this prevents redirect loops between two nodes when a primary was relocated and the relocation target is not aware
                // that it is the active primary shard already.
                request.routedBasedOnClusterVersion(state.version());
            }
            if (logger.isTraceEnabled()) {
                logger.trace("send action [{}] on primary [{}] for request [{}] with cluster state version [{}] to [{}]", opType, request
                        .shardId(), request, state.version(), primary.currentNodeId());
            }
            finished.set(true);
            rerouteAction.execute(node, request, null);
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

    private void handleErrorResponse(Throwable exp) {
        try {
            // if we got disconnected from the node, or the node / shard is not in the right state (being closed)
            Throwable cause = ExceptionsHelper.unwrapCause(exp);
            if (cause instanceof ConnectTransportException || cause instanceof NodeClosedException ||
                    retryPrimaryException(cause)) {
                logger.trace("received an error for request [{}], scheduling a retry", exp, request);
                retry(exp);
            } else {
                finishAsFailed(exp);
            }
        } catch (Throwable t) {
            onFailure(t);
        }
    }

    static boolean retryPrimaryException(Throwable e) {
        return e.getClass() == RetryOnPrimaryException.class
                || TransportActions.isShardNotAvailableException(e);
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

    void retry(Throwable failure) {
        assert failure != null;
        if (observer.isTimedOut()) {
            // we running as a last attempt after a timeout has happened. don't retry
            finishAsFailed(failure);
            return;
        }
        observer.waitForNextChange(new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {
                run();
            }

            @Override
            public void onClusterServiceClose() {
                finishAsFailed(new NodeClosedException(observer.observedState().nodes().localNode()));
            }

            @Override
            public void onTimeout(TimeValue timeout) {
                // Try one more time...
                run();
            }
        });
    }

    void finishAsFailed(Throwable failure) {
        if (finished.compareAndSet(false, true)) {
            logger.trace("operation failed. action [{}], request [{}]", failure, opType, request);
            listener.onFailure(failure);
        } else {
            assert false : "finishAsFailed called but operation is already finished";
        }
    }

    void finishWithUnexpectedFailure(Throwable failure) {
        logger.warn("unexpected error during the primary phase for action [{}], request [{}]", failure, opType, request);
        if (finished.compareAndSet(false, true)) {
            finishAsFailed(failure);
        } else {
            assert false : "finishWithUnexpectedFailure called but operation is already finished";
        }
    }

    void finishOnSuccess(Response response) {
        if (finished.compareAndSet(false, true)) {
            if (logger.isTraceEnabled()) {
                logger.trace("operation succeeded. action [{}],request [{}]", opType, request);
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
