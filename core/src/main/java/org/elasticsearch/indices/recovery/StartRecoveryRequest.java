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

package org.elasticsearch.indices.recovery;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;

/**
 *
 */
public class StartRecoveryRequest extends TransportRequest {

    private long recoveryId;

    private ShardId shardId;

    private DiscoveryNode sourceNode;

    private DiscoveryNode targetNode;

    private Store.MetadataSnapshot metadataSnapshot;

    private org.elasticsearch.cluster.routing.RecoverySource recoverySource;

    private boolean primary;

    public StartRecoveryRequest() {
    }

    /**
     * Start recovery request.
     *
     * @param sourceNode       The node to recover from
     * @param targetNode       The node to recover to
     */
    public StartRecoveryRequest(ShardId shardId, DiscoveryNode sourceNode, DiscoveryNode targetNode, Store.MetadataSnapshot metadataSnapshot, RecoverySource recoverySource, boolean primary, long recoveryId) {
        this.recoveryId = recoveryId;
        this.shardId = shardId;
        this.sourceNode = sourceNode;
        this.targetNode = targetNode;
        this.recoverySource = recoverySource;
        this.metadataSnapshot = metadataSnapshot;
        this.primary = primary;
    }

    public long recoveryId() {
        return this.recoveryId;
    }

    public ShardId shardId() {
        return shardId;
    }

    public DiscoveryNode sourceNode() {
        return sourceNode;
    }

    public DiscoveryNode targetNode() {
        return targetNode;
    }

    public RecoverySource recoveryType() {
        return recoverySource;
    }

    public boolean primary() {
        return primary;
    }

    public boolean isPrimaryRelocation() {
        return recoveryType() == RecoverySource.PRIMARY && primary();
    }

    public Store.MetadataSnapshot metadataSnapshot() {
        return metadataSnapshot;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        recoveryId = in.readLong();
        shardId = ShardId.readShardId(in);
        sourceNode = new DiscoveryNode(in);
        targetNode = new DiscoveryNode(in);
        metadataSnapshot = new Store.MetadataSnapshot(in);
        recoverySource = RecoverySource.fromId(in.readByte());
        primary = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(recoveryId);
        shardId.writeTo(out);
        sourceNode.writeTo(out);
        targetNode.writeTo(out);
        metadataSnapshot.writeTo(out);
        out.writeByte(recoverySource.id());
        out.writeBoolean(primary);
    }

}
