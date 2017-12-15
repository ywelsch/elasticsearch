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
package org.elasticsearch.discovery.zen2;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.zen2.Messages.ApplyCommit;
import org.elasticsearch.discovery.zen2.ConsensusState.CommittedState;
import org.elasticsearch.discovery.zen2.ConsensusState.NodeCollection;
import org.elasticsearch.discovery.zen2.Messages.PublishRequest;
import org.elasticsearch.discovery.zen2.Messages.PublishResponse;
import org.elasticsearch.discovery.zen2.Messages.Vote;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.io.IOException;
import java.util.Optional;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;


public class ConsensusStateTests extends ESTestCase {

    public ConsensusState<ClusterState> createInitialState(ClusterState initialClusterState) {
        return new ConsensusState<>(Settings.EMPTY, 0L, initialClusterState, Optional.empty(),
            new ConsensusState.Persistence<ClusterState>() {
                @Override
                public void persistCurrentTerm(long currentTerm) {

                }

                @Override
                public void persistCommittedState(ClusterState committedState) {

                }

                @Override
                public void persistAcceptedState(ConsensusState.AcceptedState<ClusterState> termDiff) {

                }
            });
    }

    @TestLogging("org.elasticsearch.discovery.zen2:TRACE")
    public void testSimpleScenario() {
        DiscoveryNode node1 = new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode node2 = new DiscoveryNode("node2", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode node3 = new DiscoveryNode("node3", buildNewFakeTransportAddress(), Version.CURRENT);
        NodeCollection initialConfig = new NodeCollection();
        initialConfig.add(node1);
        ClusterState initialClusterState = new ClusterState(-1L, initialConfig, 42);
        ConsensusState<ClusterState> n1 = createInitialState(initialClusterState);
        ConsensusState<ClusterState> n2 = createInitialState(initialClusterState);
        ConsensusState<ClusterState> n3 = createInitialState(initialClusterState);

        assertThat(n1.getCurrentTerm(), equalTo(0L));
        Vote v1 = n1.handleStartVote(1);
        assertThat(n1.getCurrentTerm(), equalTo(1L));

        assertThat(n2.getCurrentTerm(), equalTo(0L));
        Vote v2 = n2.handleStartVote(1);
        assertThat(n2.getCurrentTerm(), equalTo(1L));

        Optional<PublishRequest<ClusterState>> invalidVote = n1.handleVote(node2, v2);
        assertFalse(invalidVote.isPresent());

        Diff<ClusterState> diff = createUpdate("set value to 5", cs -> new ClusterState(cs.getSlot() + 1, cs.getVotingNodes(), 5));
        expectThrows(IllegalArgumentException.class, () -> n1.handleClientValue(diff));
        n1.handleVote(node1, v1);

        PublishRequest<ClusterState> slotTermDiff = n1.handleClientValue(diff);

        PublishResponse n1PublishResponse = n1.handlePublishRequest(slotTermDiff);
        expectThrows(IllegalArgumentException.class, () -> n3.handlePublishRequest(slotTermDiff));
        n3.handleStartVote(1);
        PublishResponse n3PublishResponse = n3.handlePublishRequest(slotTermDiff);

        assertFalse(n1.handlePublishResponse(node3, n3PublishResponse).isPresent());
        Optional<ApplyCommit> n1Commit = n1.handlePublishResponse(node1, n1PublishResponse);
        assertTrue(n1Commit.isPresent());

        assertThat(n1.firstUncommittedSlot(), equalTo(0L));
        n1.handleCommit(n1Commit.get());
        assertThat(n1.firstUncommittedSlot(), equalTo(1L));

        assertThat(n2.firstUncommittedSlot(), equalTo(0L));
        expectThrows(IllegalArgumentException.class, () -> n2.handleCommit(n1Commit.get()));
        assertThat(n2.firstUncommittedSlot(), equalTo(0L));

        assertThat(n3.firstUncommittedSlot(), equalTo(0L));
        assertThat(n3.getCommittedState().value, equalTo(42));
        n3.handleCommit(n1Commit.get());
        assertThat(n3.firstUncommittedSlot(), equalTo(1L));
        assertThat(n3.getCommittedState().value, equalTo(5));

        ClusterState n3ClusterState = n3.generateCatchup();
        n2.applyCatchup(n3ClusterState);
        assertThat(n2.firstUncommittedSlot(), equalTo(1L));
        assertThat(n2.getCommittedState().value, equalTo(5));
    }

    static class ClusterState implements CommittedState {

        private final long slot;
        private final NodeCollection config;
        private final int value;

        ClusterState(long slot, NodeCollection config, int value) {
            this.slot = slot;
            this.config = config;
            this.value = value;
        }

        @Override
        public long getSlot() {
            return slot;
        }

        @Override
        public NodeCollection getVotingNodes() {
            return config;
        }

        public int getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "ClusterState {slot=" + slot + ", value=" + value + ", config=" + config + "}";
        }
    }

    public static Diff<ClusterState> createUpdate(String description, Function<ClusterState, ClusterState> update) {
        return new Diff<ClusterState>() {

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                fail();
            }

            @Override
            public ClusterState apply(ClusterState part) {
                return update.apply(part);
            }

            @Override
            public String toString() {
                return description;
            }
        };
    }

}
