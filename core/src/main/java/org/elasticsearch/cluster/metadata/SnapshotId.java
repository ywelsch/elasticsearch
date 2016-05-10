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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.snapshots.Snapshot;

import java.io.IOException;
import java.util.Objects;

/**
 * SnapshotId - snapshot (repository name + snapshot name) + snapshot UUID
 */
public class SnapshotId implements Writeable {

    public static final String UNASSIGNED_UUID = ""; // empty string so its compatible with old blob naming

    private final Snapshot snapshot;
    private final String uuid;

    // Caching hash code
    private final int hashCode;

    /**
     * Constructs a new snapshot
     *
     * @param snapshot snapshot description
     * @param uuid     snapshot uuid
     */
    private SnapshotId(final Snapshot snapshot, final String uuid) {
        this.snapshot = Objects.requireNonNull(snapshot);
        this.uuid = Objects.requireNonNull(uuid);
        this.hashCode = computeHashCode();
    }

    /**
     * Constructs a new snapshot from a input stream
     *
     * @param in  input stream
     */
    public SnapshotId(final StreamInput in) throws IOException {
        snapshot = new Snapshot(in);
        uuid = in.readString();
        hashCode = computeHashCode();
    }

    /**
     * Create a new SnapshotId with a new UUID.  This should be used when getting a
     * SnapshotId for a new snapshot.
     *
     * @param snapshot snapshot
     * @return snapshot id
     */
    public static SnapshotId createNew(final Snapshot snapshot) {
        return new SnapshotId(snapshot, UUIDs.randomBase64UUID());
    }

    /**
     * Get a SnapshotId from the given components.
     *
     * @param snapshot   the snapshot description
     * @param uuid       the snapshot uuid
     * @return snapshot id
     */
    public static SnapshotId get(final Snapshot snapshot, final String uuid) {
        return new SnapshotId(snapshot, uuid);
    }

    /**
     * Returns the snapshot.
     */
    public Snapshot getSnapshot() {
        return snapshot;
    }

    /**
     * Returns repository name
     *
     * @return repository name
     */
    public String getRepository() {
        return snapshot.getRepository();
    }

    /**
     * Returns snapshot name
     *
     * @return snapshot name
     */
    public String getName() {
        return snapshot.getName();
    }

    /**
     * Returns the snapshot UUID
     *
     * @return snapshot uuid
     */
    public String getUUID() {
        return uuid;
    }

    @Override
    public String toString() {
        return snapshot.toString() + "/" + uuid;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        @SuppressWarnings("unchecked") final SnapshotId that = (SnapshotId) o;
        return snapshot.equals(that.snapshot) && uuid.equals(that.uuid);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    private int computeHashCode() {
        return Objects.hash(snapshot, uuid);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        snapshot.writeTo(out);
        out.writeString(uuid);
    }

    public String blobId() {
        if (uuid.equals(UNASSIGNED_UUID)) {
            // the old snapshot blob naming
            return snapshot.getName();
        }
        return snapshot.getName() + "-" + uuid;
    }

}
