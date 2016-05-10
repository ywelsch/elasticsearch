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

package org.elasticsearch.snapshots;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Objects;

/**
 * A snapshot, which consists of a repository name and a snapshot name.
 */
public class Snapshot implements Writeable {

    private final String repository;
    private final String name;

    /**
     * Constructs a snapshot.
     */
    public Snapshot(final String repository, final String name) {
        this.repository = Objects.requireNonNull(repository);
        this.name = Objects.requireNonNull(name);
    }

    /**
     * Constructs a snapshot from the stream input.
     */
    public Snapshot(final StreamInput in) throws IOException {
        repository = in.readString();
        name = in.readString();
    }

    /**
     * Gets the repository name for the snapshot.
     */
    public String getRepository() {
        return repository;
    }

    /**
     * Gets the snapshot name for the snapshot.
     */
    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return repository + ":" + name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        @SuppressWarnings("unchecked") Snapshot that = (Snapshot) o;
        return repository.equals(that.repository) && name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(repository, name);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeString(repository);
        out.writeString(name);
    }

}
