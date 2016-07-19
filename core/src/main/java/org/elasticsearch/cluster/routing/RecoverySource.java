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

package org.elasticsearch.cluster.routing;

public enum RecoverySource {
    /**
     * creates a new empty store
     */
    NEW_STORE((byte) 0),
    /**
     * recovery from an existing on-disk store
     */
    EXISTING_STORE((byte) 1),
    /**
     * peer recovery from a primary shard
     */
    PRIMARY((byte) 2),
    /**
     * recovery from a snapshot
     */
    SNAPSHOT((byte) 3),
    /**
     * recovery from other shards on same node (shrink index action)
     */
    LOCAL_SHARDS((byte) 4);

    private static final RecoverySource[] TYPES = new RecoverySource[RecoverySource.values().length];

    static {
        for (RecoverySource type : RecoverySource.values()) {
            assert type.id() < TYPES.length && type.id() >= 0;
            TYPES[type.id] = type;
        }
    }

    private final byte id;

    RecoverySource(byte id) {
        this.id = id;
    }

    public byte id() {
        return id;
    }

    public boolean initialRecovery() {
        return this == NEW_STORE || this == SNAPSHOT || this == LOCAL_SHARDS;
    }

    public static RecoverySource fromId(byte id) {
        if (id < 0 || id >= TYPES.length) {
            throw new IllegalArgumentException("No mapping for id [" + id + "]");
        }
        return TYPES[id];
    }
}
