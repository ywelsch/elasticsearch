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

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.snapshots.Snapshot;

import java.io.IOException;
import java.util.Objects;

public abstract class RecoverySource implements Writeable, ToXContent {

    private final Type type;

    protected RecoverySource(Type type) {
        this.type = type;
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field("type", type);
        addAdditionalFields(builder, params);
        return builder.endObject();
    }

    public abstract void addAdditionalFields(XContentBuilder builder, ToXContent.Params params) throws IOException;

    public static RecoverySource readFrom(StreamInput in) throws IOException {
        Type type = Type.fromId(in.readByte());
        switch (type) {
            case STORE: return new StoreRecoverySource(in);
            case PEER: return PeerRecoverySource.INSTANCE;
            case SNAPSHOT: return new SnapshotRecoverySource(in);
            case LOCAL_SHARDS: return LocalShardsRecoverySource.INSTANCE;
            default: throw new IllegalArgumentException("unknown recovery type: " + type.id());
        }
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeByte(type.id());
        writeAdditionalFields(out);
    }

    protected abstract void writeAdditionalFields(StreamOutput out) throws IOException;

    public enum Type {
        STORE((byte) 0),
        PEER((byte) 1),
        SNAPSHOT((byte) 2),
        LOCAL_SHARDS((byte) 3);

        private final byte id;

        Type(byte id) {
            this.id = id;
        }

        public byte id() {
            return this.id;
        }

        public static Type fromId(byte id) {
            switch (id) {
                case 0:
                    return STORE;
                case 1:
                    return PEER;
                case 2:
                    return SNAPSHOT;
                case 3:
                    return LOCAL_SHARDS;
                default:
                    throw new IllegalArgumentException("No mapping for id [" + id + "]");
            }
        }
    }

    public Type getType() {
        return type;
    }

    public boolean isStoreRecoverySource() {
        return false;
    }

    public final boolean isExistingStoreRecoverySource() {
        return isStoreRecoverySource() && asStoreRecoverySource().isFreshCopy() == false;
    }

    public final boolean isFreshStoreRecoverySource() {
        return isStoreRecoverySource() && asStoreRecoverySource().isFreshCopy();
    }

    public StoreRecoverySource asStoreRecoverySource() {
        return (StoreRecoverySource) this;
    }

    public boolean isSnapshotRecoverySource() {
        return false;
    }

    public SnapshotRecoverySource asSnapshotRecoverySource() {
        return (SnapshotRecoverySource) this;
    }

    public boolean isPeerRecoverySource() {
        return false;
    }

    public PeerRecoverySource asPeerRecoverySource() {
        return (PeerRecoverySource) this;
    }

    public boolean isLocalShardsRecoverySource() {
        return false;
    }

    public LocalShardsRecoverySource asLocalShardsRecoverySource() {
        return (LocalShardsRecoverySource) this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RecoverySource that = (RecoverySource) o;

        return type == that.type;

    }

    @Override
    public int hashCode() {
        return type.hashCode();
    }

    /**
     * recovery from an existing on-disk store or a fresh copy
     */
    public static class StoreRecoverySource extends RecoverySource {
        public static final StoreRecoverySource FRESH_COPY_INSTANCE = new StoreRecoverySource(true);
        public static final StoreRecoverySource EXISTING_COPY_INSTANCE = new StoreRecoverySource(false);

        private final boolean freshCopy;

        private StoreRecoverySource(boolean freshCopy) {
            super(Type.STORE);
            this.freshCopy = freshCopy;
        }

        StoreRecoverySource(StreamInput in) throws IOException {
            super(Type.STORE);
            freshCopy = in.readBoolean();
        }

        public boolean isFreshCopy() {
            return freshCopy;
        }

        @Override
        public void addAdditionalFields(XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.field("fresh_copy", freshCopy);
        }

        @Override
        protected void writeAdditionalFields(StreamOutput out) throws IOException {
            out.writeBoolean(freshCopy);
        }

        @Override
        public String toString() {
            return freshCopy ? "empty store recovery" : "store recovery";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            StoreRecoverySource that = (StoreRecoverySource) o;

            return freshCopy == that.freshCopy;

        }

        @Override
        public int hashCode() {
            return (freshCopy ? 1 : 0);
        }

        @Override
        public boolean isStoreRecoverySource() {
            return true;
        }
    }

    /**
     * recovery from other shards on same node (shrink index action)
     */
    public static class LocalShardsRecoverySource extends RecoverySource {

        public static final LocalShardsRecoverySource INSTANCE = new LocalShardsRecoverySource();

        private LocalShardsRecoverySource() {
            super(Type.LOCAL_SHARDS);
        }

        @Override
        public void addAdditionalFields(XContentBuilder builder, ToXContent.Params params) throws IOException {

        }

        @Override
        protected void writeAdditionalFields(StreamOutput out) throws IOException {

        }

        @Override
        public String toString() {
            return "local shards recovery";
        }

        @Override
        public boolean isLocalShardsRecoverySource() {
            return true;
        }
    }

    /**
     * recovery from a snapshot
     */
    public static class SnapshotRecoverySource extends RecoverySource {
        private final Snapshot snapshot;
        private final String index;
        private final Version version;

        public SnapshotRecoverySource(Snapshot snapshot, Version version, String index) {
            super(Type.SNAPSHOT);
            this.snapshot = Objects.requireNonNull(snapshot);
            this.version = Objects.requireNonNull(version);
            this.index = Objects.requireNonNull(index);
        }

        SnapshotRecoverySource(StreamInput in) throws IOException {
            super(Type.SNAPSHOT);
            snapshot = new Snapshot(in);
            version = Version.readVersion(in);
            index = in.readString();
        }

        public Snapshot snapshot() {
            return snapshot;
        }

        public String index() {
            return index;
        }

        public Version version() {
            return version;
        }

        @Override
        protected void writeAdditionalFields(StreamOutput out) throws IOException {
            snapshot.writeTo(out);
            Version.writeVersion(version, out);
            out.writeString(index);
        }

        @Override
        public void addAdditionalFields(XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.field("repository", snapshot.getRepository())
                .field("snapshot", snapshot.getSnapshotId().getName())
                .field("version", version.toString())
                .field("index", index);
        }

        @Override
        public String toString() {
            return "snapshot recovery from " + snapshot.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            @SuppressWarnings("unchecked") SnapshotRecoverySource that = (SnapshotRecoverySource) o;
            return snapshot.equals(that.snapshot) && index.equals(that.index) && version.equals(that.version);
        }

        @Override
        public int hashCode() {
            return Objects.hash(snapshot, index, version);
        }

        @Override
        public boolean isSnapshotRecoverySource() {
            return true;
        }
    }

    /**
     * peer recovery from a primary shard
     */
    public static class PeerRecoverySource extends RecoverySource {

        public static final PeerRecoverySource INSTANCE = new PeerRecoverySource();

        private PeerRecoverySource() {
            super(Type.PEER);
        }

        @Override
        public void addAdditionalFields(XContentBuilder builder, ToXContent.Params params) throws IOException {

        }

        @Override
        protected void writeAdditionalFields(StreamOutput out) throws IOException {

        }

        @Override
        public String toString() {
            return "peer recovery";
        }

        @Override
        public boolean isPeerRecoverySource() {
            return true;
        }
    }
}
