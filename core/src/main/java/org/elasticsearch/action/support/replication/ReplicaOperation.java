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
import org.elasticsearch.action.ReplicationResponse;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.NodeClosedException;

import java.io.IOException;
import java.util.function.Consumer;

public abstract class ReplicaOperation<Request extends ReplicationRequest<Request>, ReplicaRequest extends
        ReplicationRequest<ReplicaRequest>, Response extends ReplicationResponse> extends AbstractRunnable {
    private final ReplicaRequest request;
    private final IndexShardReference<Request, ReplicaRequest, Response> shardReference;
    private final ClusterStateObserver observer;
    private final ESLogger logger;
    private final String opType;
    private final Consumer<Runnable> executor;

    ReplicaOperation(ReplicaRequest request, IndexShardReference<Request, ReplicaRequest, Response> shardReference, ClusterStateObserver
            observer, ESLogger logger, String opType, Consumer<Runnable> executor) {
        this.request = request;
        this.shardReference = shardReference;
        this.observer = observer;
        this.logger = logger;
        this.opType = opType;
        this.executor = executor;
    }

    abstract protected void finishAsFailed(Throwable t);

    abstract protected void finishAsSuccessful();


    @Override
    public void onFailure(Throwable t) {
        try {
            failReplicaIfNeeded(t);
        } catch (Throwable unexpected) {
            logger.error("{} unexpected error while failing replica", unexpected, request.shardId().id());
        } finally {
            finishAsFailed(t);
        }
    }

    private void retry(Throwable t) {
        logger.trace("Retrying operation on replica, op [{}], request [{}]", t, opType, request);
        observer.waitForNextChange(new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {
                // Forking a thread on local node via transport service so that custom transport service have an
                // opportunity to execute custom logic before the replica operation begins
                String extraMessage = "action [" + opType + "], request[" + request + "]";
                executor.accept(ReplicaOperation.this);
            }

            @Override
            public void onClusterServiceClose() {
                finishAsFailed(new NodeClosedException(observer.observedState().nodes().localNode()));
            }

            @Override
            public void onTimeout(TimeValue timeout) {
                throw new AssertionError("Cannot happen: there is not timeout");
            }
        });
    }

    private void failReplicaIfNeeded(Throwable t) {
        String index = request.shardId().getIndex().getName();
        int shardId = request.shardId().id();
        logger.trace("failure on replica [{}][{}], action [{}], request [{}]", t, index, shardId, opType, request);
        if (ignoreReplicaException(t) == false) {
            shardReference.failShard(opType + " failed on replica", t);
        }
    }

    @Override
    protected void doRun() throws Exception {
        try {
            shardReference.shardOperationOnReplica(request);
            finishAsSuccessful();
        } catch (RetryOnReplicaException retryException) {
            retry(retryException);
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

    /**
     * Should an exception be ignored when the operation is performed on the replica.
     */
    static boolean ignoreReplicaException(Throwable e) {
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

    static boolean isConflictException(Throwable e) {
        Throwable cause = ExceptionsHelper.unwrapCause(e);
        // on version conflict or document missing, it means
        // that a new change has crept into the replica, and it's fine
        if (cause instanceof VersionConflictEngineException) {
            return true;
        }
        return false;
    }


}