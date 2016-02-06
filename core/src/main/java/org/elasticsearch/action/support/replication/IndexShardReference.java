package org.elasticsearch.action.support.replication;

import org.elasticsearch.action.ReplicationResponse;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.lease.Releasable;

interface IndexShardReference<Request extends ReplicationRequest<Request>, ReplicaRequest extends ReplicationRequest<ReplicaRequest>,
        Response extends ReplicationResponse> extends Releasable {
    boolean isRelocated();

    ShardRouting routingEntry();


    void shardOperationOnReplica(ReplicaRequest request);

    void failShard(String s, Throwable t);

    /**
     * Primary operation on node with primary copy, the provided metadata should be used for request validation if needed
     *
     * @return A tuple containing not null values, as first value the result of the primary operation and as second value
     * the request to be executed on the replica shards.
     */
    Tuple<Response, ReplicaRequest> shardOperationOnPrimary(MetaData metaData, Request request) throws Exception;
}
