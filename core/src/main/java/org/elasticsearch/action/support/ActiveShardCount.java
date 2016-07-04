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

package org.elasticsearch.action.support;

import com.carrotsearch.hppc.cursors.IntObjectCursor;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.index.IndexSettings.WAIT_FOR_ACTIVE_SHARDS_SETTING;

/**
 * A class whose instances represent a value for counting the number
 * of active shard copies for a given shard in an index.
 */
public final class ActiveShardCount implements Writeable {

    private static final int ACTIVE_SHARD_COUNT_DEFAULT = -2;
    private static final int ALL_ACTIVE_SHARDS = -1;

    public static final ActiveShardCount DEFAULT = new ActiveShardCount(ACTIVE_SHARD_COUNT_DEFAULT);
    public static final ActiveShardCount ALL = new ActiveShardCount(ALL_ACTIVE_SHARDS);
    public static final ActiveShardCount NONE = new ActiveShardCount(0);
    public static final ActiveShardCount ONE = new ActiveShardCount(1);

    private final int value;

    private ActiveShardCount(final int value) {
        this.value = value;
    }

    /**
     * Get an ActiveShardCount instance for the given value.  The value is first validated to ensure
     * it is a valid shard count and throws an IllegalArgumentException if validation fails.  Valid
     * values are any non-negative number.  Directly use {@link ActiveShardCount#DEFAULT} for the
     * default value (which is half the shards) or {@link ActiveShardCount#ALL} to specify all the shards.
     */
    public static ActiveShardCount from(final int value) {
        if (value < 0) {
            throw new IllegalArgumentException("shard count cannot be a negative value");
        }
        return get(value);
    }

    private static ActiveShardCount get(final int value) {
        switch (validateValue(value)) {
            case ACTIVE_SHARD_COUNT_DEFAULT:
                return DEFAULT;
            case ALL_ACTIVE_SHARDS:
                return ALL;
            case 0:
                return NONE;
            default:
                return new ActiveShardCount(value);
        }
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeInt(value);
    }

    public static ActiveShardCount readFrom(final StreamInput in) throws IOException {
        return get(in.readInt());
    }

    private static int validateValue(final int value) {
        if (value < 0 && value != ACTIVE_SHARD_COUNT_DEFAULT && value != ALL_ACTIVE_SHARDS) {
            throw new IllegalArgumentException("Invalid ActiveShardCount[" + value + "]");
        }
        return value;
    }

    /**
     * Resolve this instance to an actual integer value for the number of active shard counts.
     * If {@link ActiveShardCount#ALL} are specified, then the given {@link IndexMetaData} is
     * used to determine what the actual active shard count should be.  The default value indicates
     * one active shard.
     */
    public int resolve(final IndexMetaData indexMetaData) {
        Objects.requireNonNull(indexMetaData);
        if (this == ActiveShardCount.DEFAULT) {
            return 1;
        } else if (this == ActiveShardCount.ALL) {
            return indexMetaData.getNumberOfReplicas() + 1;
        } else {
            return value;
        }
    }

    /**
     * Parses the active shard count from the given string.  Valid values are "all" for
     * all shard copies, null for the default value set by {@link IndexSettings#WAIT_FOR_ACTIVE_SHARDS_SETTING}
     * (which defaults to half the shard copies, or a numeric value greater than or equal to 0.
     * Any other input will throw an IllegalArgumentException.
     */
    public static ActiveShardCount parseString(final String str) {
        if (str == null) {
            return ActiveShardCount.DEFAULT;
        } else if (str.equals("all")) {
            return ActiveShardCount.ALL;
        } else {
            int val;
            try {
                val = Integer.parseInt(str);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("cannot parse ActiveShardCount[" + str + "]");
            }
            return ActiveShardCount.from(val);
        }
    }

    /**
     * Returns true iff the given cluster state's routing table contains enough active
     * shards to meet the required shard count represented by this instance.
     */
    public boolean enoughShardsActive(final ClusterState clusterState, final String indexName, final Settings settings) {
        Objects.requireNonNull(clusterState);
        Objects.requireNonNull(indexName);
        Objects.requireNonNull(settings);
        if (this == ActiveShardCount.NONE) {
            // not waiting for any active shards
            return true;
        }
        final ActiveShardCount waitForActiveShards;
        if (this == ActiveShardCount.DEFAULT) {
            waitForActiveShards = WAIT_FOR_ACTIVE_SHARDS_SETTING.get(settings);
        } else {
            waitForActiveShards = this;
        }
        final IndexRoutingTable indexRoutingTable = clusterState.routingTable().index(indexName);
        assert indexRoutingTable != null;
        if (indexRoutingTable.allPrimaryShardsActive() == false) {
            // all primary shards aren't active yet
            return false;
        }
        final IndexMetaData indexMetaData = clusterState.metaData().index(indexName);
        assert indexMetaData != null;
        for (final IntObjectCursor<IndexShardRoutingTable> shardRouting : indexRoutingTable.getShards()) {
            if (waitForActiveShards.checkWaitForActiveShards(shardRouting.value, indexMetaData) == false) {
                // not enough active shard copies yet
                return false;
            }
        }
        return true;
    }

    /**
     * Returns true iff the active shard count in the shard routing table is enough
     * to meet the required shard count represented by this instance.
     */
    public boolean checkWaitForActiveShards(final IndexShardRoutingTable shardRoutingTable, final IndexMetaData indexMetaData) {
        Objects.requireNonNull(shardRoutingTable);
        Objects.requireNonNull(indexMetaData);
        if (shardRoutingTable.activeShards().size() < resolve(indexMetaData)) {
            // not enough active shard copies yet
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        @SuppressWarnings("unchecked") ActiveShardCount that = (ActiveShardCount) o;
        return value == that.value;
    }

    @Override
    public String toString() {
        final String valStr;
        switch (value) {
            case ALL_ACTIVE_SHARDS:
                valStr = "ALL";
                break;
            case ACTIVE_SHARD_COUNT_DEFAULT:
                valStr = "DEFAULT";
                break;
            default:
                valStr = Integer.toString(value);
        }
        return "ActiveShardCount[" + valStr + "]";
    }

}
