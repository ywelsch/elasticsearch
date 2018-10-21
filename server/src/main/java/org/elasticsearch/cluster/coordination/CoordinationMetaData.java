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
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class CoordinationMetaData implements Writeable, ToXContentFragment {

    public static final CoordinationMetaData EMPTY_META_DATA = builder().build();

    private final long term;

    private final VotingConfiguration lastCommittedConfiguration;

    private final VotingConfiguration lastAcceptedConfiguration;

    private final int masterNodesFailureTolerance;

    private final Set<String> retiredNodes;

    private final Set<DiscoveryNode> votingTombstones;

    CoordinationMetaData(long term, VotingConfiguration lastCommittedConfiguration, VotingConfiguration lastAcceptedConfiguration,
        int masterNodesFailureTolerance, Set<String> retiredNodes, Set<DiscoveryNode> votingTombstones) {
        final int safeConfigurationSize = 2 * masterNodesFailureTolerance + 1;
        if (masterNodesFailureTolerance > 0 && lastCommittedConfiguration.getNodeIds().size() < safeConfigurationSize) {
            throw new IllegalArgumentException(lastCommittedConfiguration + " is smaller than expected " + safeConfigurationSize);
        }
        if (masterNodesFailureTolerance > 0 && lastAcceptedConfiguration.getNodeIds().size() < safeConfigurationSize) {
            throw new IllegalArgumentException(lastAcceptedConfiguration + " is smaller than expected " + safeConfigurationSize);
        }
        this.term = term;
        this.lastCommittedConfiguration = lastCommittedConfiguration;
        this.lastAcceptedConfiguration = lastAcceptedConfiguration;
        this.masterNodesFailureTolerance = masterNodesFailureTolerance;
        this.retiredNodes = retiredNodes;
        this.votingTombstones = votingTombstones;
    }

    public CoordinationMetaData(StreamInput in) throws IOException {
        term = in.readLong();
        lastCommittedConfiguration = new VotingConfiguration(in);
        lastAcceptedConfiguration = new VotingConfiguration(in);
        masterNodesFailureTolerance = in.readVInt();
        retiredNodes = Collections.unmodifiableSet(Sets.newHashSet(in.readStringArray()));
        votingTombstones = Collections.unmodifiableSet(in.readSet(DiscoveryNode::new));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(CoordinationMetaData coordinationMetaData) {
        return new Builder(coordinationMetaData);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(term);
        lastCommittedConfiguration.writeTo(out);
        lastAcceptedConfiguration.writeTo(out);
        out.writeVInt(masterNodesFailureTolerance);
        out.writeStringArray(retiredNodes.toArray(new String[retiredNodes.size()]));
        out.writeCollection(votingTombstones, (o, v) -> v.writeTo(o));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder
            .field("term", term)
            .field("last_committed_config", lastCommittedConfiguration)
            .field("last_accepted_config", lastAcceptedConfiguration)
            .field("master_nodes_failure_tolerance", masterNodesFailureTolerance)
            .array("retired_nodes", retiredNodes.toArray(new String[retiredNodes.size()]));
        // TODO include voting tombstones here
    }

    public long term() {
        return term;
    }

    public VotingConfiguration getLastAcceptedConfiguration() {
        return lastAcceptedConfiguration;
    }

    public VotingConfiguration getLastCommittedConfiguration() {
        return lastCommittedConfiguration;
    }

    /**
     * The cluster usually requires a vote from at least half of the master nodes in order to commit a
     * cluster state update, and to achieve this it makes automatic adjustments to the quorum size as
     * master nodes join or leave the cluster. However, if master nodes leave the cluster slowly enough
     * then these automatic adjustments can end up with a single master node; if this last node were to
     * fail then the cluster would be rendered permanently unavailable. Instead it may be preferable to
     * stop processing cluster state updates and become unavailable when the second-last (more generally,
     * n'th-last) node leaves the cluster, so that the cluster is never in a situation where a single
     * node's failure can cause permanent unavailability. This parameter determines the size of the
     * smallest set of master nodes required to process a cluster state update.
     */
    public int masterNodesFailureTolerance() {
        return masterNodesFailureTolerance;
    }

    /**
     * Nodes that are leaving the cluster and which should not appear in the configuration if possible. Nodes that are
     * retired and not in the current configuration will never appear in the resulting configuration; this is useful
     * for shifting the vote in a 2-node cluster so one of the nodes can be restarted without harming availability.
     */
    public Set<String> retiredNodes() {
        return retiredNodes;
    }

    public Set<DiscoveryNode> getVotingTombstones() {
        return votingTombstones;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CoordinationMetaData)) return false;

        CoordinationMetaData that = (CoordinationMetaData) o;

        if (term != that.term) return false;
        if (masterNodesFailureTolerance != that.masterNodesFailureTolerance) return false;
        if (!lastCommittedConfiguration.equals(that.lastCommittedConfiguration)) return false;
        if (!lastAcceptedConfiguration.equals(that.lastAcceptedConfiguration)) return false;
        return retiredNodes.equals(that.retiredNodes);
    }

    @Override
    public int hashCode() {
        int result = (int) (term ^ (term >>> 32));
        result = 31 * result + lastCommittedConfiguration.hashCode();
        result = 31 * result + lastAcceptedConfiguration.hashCode();
        result = 31 * result + masterNodesFailureTolerance;
        result = 31 * result + retiredNodes.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "CoordinationMetaData{" +
            "term=" + term +
            ", lastCommittedConfiguration=" + lastCommittedConfiguration +
            ", lastAcceptedConfiguration=" + lastAcceptedConfiguration +
            ", masterNodesFailureTolerance=" + masterNodesFailureTolerance +
            ", retiredNodes=" + retiredNodes +
            '}';
    }

    public static class Builder {
        private long term = 0;
        private VotingConfiguration lastCommittedConfiguration = VotingConfiguration.EMPTY_CONFIG;
        private VotingConfiguration lastAcceptedConfiguration = VotingConfiguration.EMPTY_CONFIG;
        private int masterNodesFailureTolerance = 0;
        private Set<String> retiredNodes = new HashSet<>();
        private final Set<DiscoveryNode> votingTombstones = new HashSet<>();

        public Builder() {

        }
        
        public Builder(CoordinationMetaData state) {
            this.term = state.term;
            this.lastCommittedConfiguration = state.lastCommittedConfiguration;
            this.lastAcceptedConfiguration = state.lastAcceptedConfiguration;
            this.masterNodesFailureTolerance = state.masterNodesFailureTolerance;
            this.retiredNodes = new HashSet<>(state.retiredNodes);
        }

        public Builder term(long term) {
            this.term = term;
            return this;
        }

        public Builder lastCommittedConfiguration(VotingConfiguration config) {
            this.lastCommittedConfiguration = config;
            return this;
        }

        public Builder lastAcceptedConfiguration(VotingConfiguration config) {
            this.lastAcceptedConfiguration = config;
            return this;
        }

        public Builder masterNodesFailureTolerance(int masterNodesFailureTolerance) {
            this.masterNodesFailureTolerance = masterNodesFailureTolerance;
            return this;
        }

        public Builder retiredNodes(Set<String> retiredNodes) {
            this.retiredNodes = retiredNodes;
            return this;
        }

        public Builder addVotingTombstone(DiscoveryNode tombstone) {
            votingTombstones.add(tombstone);
            return this;
        }

        public Builder clearVotingTombstones() {
            votingTombstones.clear();
            return this;
        }

        public CoordinationMetaData build() {
            return new CoordinationMetaData(term, lastCommittedConfiguration, lastAcceptedConfiguration,
                masterNodesFailureTolerance, Collections.unmodifiableSet(new HashSet<>(retiredNodes)),
                Collections.unmodifiableSet(new HashSet<>(votingTombstones)));
        }
    }

    /**
     * A collection of persistent node ids, denoting the voting configuration for cluster state changes.
     */
    public static class VotingConfiguration implements Writeable, ToXContentFragment {

        public static final VotingConfiguration EMPTY_CONFIG = new VotingConfiguration(Collections.emptySet());

        private final Set<String> nodeIds;

        public VotingConfiguration(Set<String> nodeIds) {
            this.nodeIds = Collections.unmodifiableSet(new HashSet<>(nodeIds));
        }

        public VotingConfiguration(StreamInput in) throws IOException {
            nodeIds = Collections.unmodifiableSet(Sets.newHashSet(in.readStringArray()));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeStringArray(nodeIds.toArray(new String[nodeIds.size()]));
        }

        public boolean hasQuorum(Collection<String> votes) {
            final HashSet<String> intersection = new HashSet<>(nodeIds);
            intersection.retainAll(votes);
            return intersection.size() * 2 > nodeIds.size();
        }

        public Set<String> getNodeIds() {
            return nodeIds;
        }

        @Override
        public String toString() {
            return "VotingConfiguration{" + String.join(",", nodeIds) + "}";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            VotingConfiguration that = (VotingConfiguration) o;
            return Objects.equals(nodeIds, that.nodeIds);
        }

        @Override
        public int hashCode() {
            return Objects.hash(nodeIds);
        }

        public boolean isEmpty() {
            return nodeIds.isEmpty();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startArray();
            for (String nodeId : nodeIds) {
                builder.value(nodeId);
            }
            return builder.endArray();
        }

        public static VotingConfiguration of(DiscoveryNode... nodes) {
            // this could be used in many more places - TODO use this where appropriate
            return new VotingConfiguration(Arrays.stream(nodes).map(DiscoveryNode::getId).collect(Collectors.toSet()));
        }
    }
}
