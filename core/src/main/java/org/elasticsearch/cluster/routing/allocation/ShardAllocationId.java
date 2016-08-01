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

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.index.shard.ShardId;

/**
 * Wraps shard id and allocation id into a class to refer to a specific shard allocation.
 */
public class ShardAllocationId {
    protected final ShardId shardId;
    protected final String allocationId;

    public ShardAllocationId(ShardId shardId, String allocationId) {
        this.shardId = shardId;
        this.allocationId = allocationId;
    }

    public ShardAllocationId(ShardRouting shardRouting) {
        this.shardId = shardRouting.shardId();
        this.allocationId = shardRouting.allocationId().getId();
    }

    public ShardId shardId() {
        return shardId;
    }

    public String allocationId() {
        return allocationId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ShardAllocationId that = (ShardAllocationId) o;

        if (!shardId.equals(that.shardId)) return false;
        return allocationId.equals(that.allocationId);

    }

    @Override
    public int hashCode() {
        int result = shardId.hashCode();
        result = 31 * result + allocationId.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return shardId + "[" + allocationId + "]";
    }
}
