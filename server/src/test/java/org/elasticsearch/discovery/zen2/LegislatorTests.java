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

import com.carrotsearch.randomizedtesting.RandomizedContext;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterState.Builder;
import org.elasticsearch.cluster.ClusterState.VotingConfiguration;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNode.Role;
import org.elasticsearch.cluster.service.ClusterApplier;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.Discovery.AckListener;
import org.elasticsearch.discovery.zen.MembershipAction;
import org.elasticsearch.discovery.zen2.ConsensusState.BasePersistedState;
import org.elasticsearch.discovery.zen2.ConsensusState.PersistedState;
import org.elasticsearch.discovery.zen2.Legislator.Mode;
import org.elasticsearch.discovery.zen2.Legislator.Transport;
import org.elasticsearch.discovery.zen2.LegislatorTests.Cluster.ClusterNode;
import org.elasticsearch.discovery.zen2.Messages.AbdicationRequest;
import org.elasticsearch.discovery.zen2.Messages.ApplyCommit;
import org.elasticsearch.discovery.zen2.Messages.HeartbeatRequest;
import org.elasticsearch.discovery.zen2.Messages.HeartbeatResponse;
import org.elasticsearch.discovery.zen2.Messages.Join;
import org.elasticsearch.discovery.zen2.Messages.LeaderCheckRequest;
import org.elasticsearch.discovery.zen2.Messages.LeaderCheckResponse;
import org.elasticsearch.discovery.zen2.Messages.LegislatorPublishResponse;
import org.elasticsearch.discovery.zen2.Messages.OfferJoin;
import org.elasticsearch.discovery.zen2.Messages.PrejoinHandoverRequest;
import org.elasticsearch.discovery.zen2.Messages.PublishRequest;
import org.elasticsearch.discovery.zen2.Messages.SeekJoins;
import org.elasticsearch.discovery.zen2.Messages.StartJoinRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static org.elasticsearch.discovery.zen2.ConsensusStateTests.clusterState;
import static org.elasticsearch.discovery.zen2.ConsensusStateTests.value;
import static org.elasticsearch.discovery.zen2.ConsensusStateTests.setValue;
import static org.elasticsearch.discovery.zen2.Legislator.CONSENSUS_HEARTBEAT_DELAY_SETTING;
import static org.elasticsearch.discovery.zen2.Legislator.CONSENSUS_HEARTBEAT_TIMEOUT_SETTING;
import static org.elasticsearch.discovery.zen2.Legislator.CONSENSUS_LEADER_CHECK_RETRY_COUNT_SETTING;
import static org.elasticsearch.discovery.zen2.Legislator.CONSENSUS_MIN_DELAY_SETTING;
import static org.elasticsearch.discovery.zen2.Legislator.CONSENSUS_PUBLISH_TIMEOUT_SETTING;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.core.Is.is;

public class LegislatorTests extends ESTestCase {

    public void testCanProposeValueAfterStabilisation() {
        final Cluster cluster = new Cluster(randomIntBetween(1, 5));
        cluster.runRandomly(true);
        cluster.stabilise();
        final ClusterNode leader = cluster.getAnyLeader();
        final long stabilisedVersion = leader.legislator.getLastCommittedState().get().getVersion();

        final long finalValue = randomInt();
        logger.info("--> proposing final value [{}] to [{}]", finalValue, leader.getId());
        leader.submitValue(finalValue);
        cluster.stabilise(Cluster.DEFAULT_DELAY_VARIABILITY, 0L);

        for (final ClusterNode clusterNode : cluster.clusterNodes) {
            final String legislatorId = clusterNode.getId();
            final ClusterState committedState = clusterNode.legislator.getLastCommittedState().get();
            assertThat(legislatorId + " is at the next version", committedState.getVersion(), equalTo(stabilisedVersion + 1));
            assertThat(legislatorId + " has the right value", value(committedState), is(finalValue));
            final ClusterState appliedState = clusterNode.getLastAppliedClusterState();
            assertThat(legislatorId + " has applied the latest cluster state", appliedState.getVersion(), equalTo(stabilisedVersion + 1));
        }
    }

    public void testNodeJoiningAndLeaving() {
        final Cluster cluster = new Cluster(randomIntBetween(1, 3));
        cluster.runRandomly(true);
        cluster.stabilise();

        for (final ClusterNode clusterNode : cluster.clusterNodes) {
            final ClusterState committedState = clusterNode.legislator.getLastCommittedState().get();
            assertThat(clusterNode.getId() + " misses nodes in cluster state: " + committedState.getNodes(),
                committedState.getNodes().getSize(), equalTo(cluster.getNodeCount()));
        }

        cluster.addNodes(randomIntBetween(1, 3));
        if (randomBoolean()) {
            cluster.runRandomly(true);
        }
        cluster.stabilise();

        for (final ClusterNode clusterNode : cluster.clusterNodes) {
            final ClusterState committedState = clusterNode.legislator.getLastCommittedState().get();
            assertThat(clusterNode.getId() + " misses nodes in cluster state: " + committedState.getNodes(),
                committedState.getNodes().getSize(), equalTo(cluster.getNodeCount()));
        }

        // pick a subset of nodes to disconnect so that the remaining connected nodes still have a voting quorum
        final ClusterNode leader = cluster.getAnyLeader();

        VotingConfiguration config = leader.legislator.getLastCommittedState().get().getLastCommittedConfiguration();
        Set<String> quorumOfVotingNodes = randomUnique(() -> randomFrom(config.getNodeIds()), config.getNodeIds().size() / 2 + 1);
        assertTrue(config.hasQuorum(quorumOfVotingNodes));

        Set<String> allNodes = cluster.clusterNodes.stream()
            .map(ClusterNode::getLocalNode).map(DiscoveryNode::getId).collect(Collectors.toSet());

        Set<String> nodesToDisconnect = Sets.difference(allNodes, quorumOfVotingNodes);

        if (nodesToDisconnect.isEmpty() == false) {
            nodesToDisconnect.forEach(nodeId -> {
                ClusterNode node = cluster.clusterNodes.stream().filter(cn -> cn.getLocalNode().getId().equals(nodeId)).findFirst().get();
                logger.info("--> disconnecting {}", node.getLocalNode());
                node.isConnected = false;
            });

            if (nodesToDisconnect.contains(leader.getLocalNode().getId())) {
                // currently stabilization is not long enough for the leader to be able to step down as it will take
                // ActiveLeaderFailureDetector some time to notice that the other nodes are unavailable, which will then trigger a
                // cluster state update to remove these nodes. As the leader is isolated, publishing that cluster state will time out
                // and only then will the leader step down.
                // TODO: improve failure detection
                cluster.stabilise(2 * cluster.DEFAULT_STABILISATION_TIME, Cluster.DEFAULT_DELAY_VARIABILITY);
            } else {
                cluster.stabilise();
            }

            assertThat(cluster.getAnyLeader().legislator.getLastCommittedState().get().nodes().getSize(),
                equalTo(cluster.getNodeCount() - nodesToDisconnect.size()));
        }
    }

    public void testCanAbdicateAfterStabilisation() {
        final Cluster cluster = new Cluster(randomIntBetween(1, 5));
        cluster.runRandomly(true);
        cluster.stabilise();
        final ClusterNode leader = cluster.getAnyLeader();
        final long stabilisedVersion = leader.legislator.getLastCommittedState().get().getVersion();

        final ClusterNode newLeader = cluster.randomLegislator();
        logger.info("--> abdicating from [{}] to [{}]", leader.getId(), newLeader.getId());
        cluster.setDelayVariability(0L);
        leader.legislator.abdicateTo(newLeader.localNode);
        cluster.stabilise(0L, 0L);

        for (final ClusterNode clusterNode : cluster.clusterNodes) {
            final String legislatorId = clusterNode.getId();
            final Legislator legislator = clusterNode.legislator;
            assertThat(legislatorId + " is at the next version", legislator.getLastCommittedState().get().getVersion(),
                greaterThan(stabilisedVersion));
            if (clusterNode == newLeader) {
                assertThat(legislatorId + " is the leader", legislator.getMode(), is(Legislator.Mode.LEADER));
            } else {
                assertThat(legislatorId + " is a follower", legislator.getMode(), is(Legislator.Mode.FOLLOWER));
            }
        }
    }

    public void testStabilisationWithDisconnectedLeader() {
        final Cluster cluster = createStabilisedThreeNodeCluster();
        final ClusterNode leader = cluster.getAnyLeader();

        leader.isConnected = false;
        if (randomBoolean()) {
            cluster.runRandomly(false);
        }
        // currently stabilization is not long enough for the leader to be able to step down as it will take
        // ActiveLeaderFailureDetector some time to notice that the other nodes are unavailable, which will then trigger a
        // cluster state update to remove these nodes. As the leader is isolated, publishing that cluster state will time out
        // and only then will the leader step down.
        // TODO: improve failure detection
        cluster.stabilise(2 * cluster.DEFAULT_STABILISATION_TIME, Cluster.DEFAULT_DELAY_VARIABILITY);

        final ClusterNode newLeader = cluster.getAnyLeader();
        assertNotEquals(leader, newLeader);
        assertTrue(newLeader.isConnected);
    }

    public void testFastElectionWhenLeaderDropsConnections() {
        final Cluster cluster = createStabilisedThreeNodeCluster();
        final ClusterNode leader = cluster.getAnyLeader();

        leader.isConnected = false;

        for (ClusterNode clusterNode : cluster.clusterNodes) {
            if (clusterNode != leader) {
                logger.info("--> notifying {} of leader disconnection", clusterNode.getLocalNode());
                clusterNode.legislator.handleDisconnectedNode(leader.localNode);
            }
        }

        logger.info("--> failing old leader");
        leader.legislator.handleFailure();
        logger.info("--> finished failing old leader");

        cluster.stabilise(cluster.DEFAULT_ELECTION_TIME, Cluster.DEFAULT_DELAY_VARIABILITY);
    }

    public void testFastFailureWhenAQuorumRebootsUncleanly() {
        final Cluster cluster = createStabilisedThreeNodeCluster();
        final ClusterNode leader = cluster.getAnyLeader();
        final ClusterNode follower0 = cluster.getAnyNodeExcept(leader);
        final ClusterNode follower1 = cluster.getAnyNodeExcept(leader, follower0);

        follower0.isConnected = false;
        follower1.isConnected = false;

        logger.info("--> nodes disconnected - starting publication");

        leader.submitRandomValue();

        logger.info("--> publication started - now rebooting nodes");

        for (final ClusterNode clusterNode : cluster.clusterNodes) {
            if (clusterNode.equals(leader) == false) {
                DiscoveryNode oldLocalNode = clusterNode.localNode;
                clusterNode.reboot();
                clusterNode.isConnected = true;
                leader.futureExecutor.schedule(TimeValue.ZERO, "handle disconnect",
                    () -> leader.legislator.handleDisconnectedNode(oldLocalNode));
            }
        }

        logger.info("--> nodes rebooted - now stabilising");

        cluster.stabilise(cluster.DEFAULT_ELECTION_TIME, Cluster.DEFAULT_DELAY_VARIABILITY);

        DiscoveryNodes finalDiscoveryNodes = leader.legislator.getLastCommittedState().get().getNodes();
        for (final ClusterNode clusterNode : cluster.clusterNodes) {
            assertTrue(clusterNode.localNode + " is in cluster state", finalDiscoveryNodes.nodeExists(clusterNode.localNode));
        }
    }

    public void testFastRemovalWhenFollowerDropsConnections() {
        final Cluster cluster = createStabilisedThreeNodeCluster();
        final ClusterNode leader = cluster.getAnyLeader();
        final ClusterNode follower0 = cluster.getAnyNodeExcept(leader);

        assertThat(leader.getLastAppliedClusterState().nodes().get(follower0.localNode.getId()), not(nullValue()));
        long lastAppliedVersion = leader.getLastAppliedClusterState().version();

        follower0.isConnected = false;

        logger.info("--> notifying {} of node disconnection", leader.getLocalNode());
        leader.legislator.handleDisconnectedNode(follower0.localNode);

        logger.info("--> failing {}", follower0.getLocalNode());
        follower0.legislator.handleFailure();

        cluster.stabilise(Cluster.DEFAULT_DELAY_VARIABILITY, 0L);

        assertThat(leader.getLastAppliedClusterState().nodes().get(follower0.localNode.getId()), nullValue());
        assertThat(leader.getLastAppliedClusterState().version(), is(lastAppliedVersion + 1));
    }

    public void testLagDetectionCausesRejoin() {
        final Cluster cluster = createStabilisedThreeNodeCluster();
        final ClusterNode leader = cluster.getAnyLeader();
        final ClusterNode follower0 = cluster.getAnyNodeExcept(leader);

        logger.info("--> disconnecting {}", follower0.getLocalNode());
        follower0.isConnected = false;

        leader.submitRandomValue();
        cluster.runUntil(cluster.currentTimeMillis + Cluster.DEFAULT_DELAY_VARIABILITY);

        logger.info("--> reconnecting {}", follower0.getLocalNode());
        follower0.isConnected = true;

        cluster.stabilise();
    }

    public void testAckListenerReceivesAcksFromAllNodes() {
        final Cluster cluster = createStabilisedThreeNodeCluster();
        final ClusterNode leader = cluster.getAnyLeader();

        AckCollector ackCollector = leader.submitRandomValue();
        cluster.stabilise(Cluster.DEFAULT_DELAY_VARIABILITY, 0L);
        for (final ClusterNode clusterNode : cluster.clusterNodes) {
            assertTrue("expected ack from " + clusterNode, ackCollector.hasAckedSuccessfully(clusterNode));
        }
        assertThat("leader should be last to ack", ackCollector.getSuccessfulAckIndex(leader), equalTo(2));
    }

    public void testAckListenerReceivesNackFromFollower() {
        final Cluster cluster = createStabilisedThreeNodeCluster();
        final ClusterNode leader = cluster.getAnyLeader();
        final ClusterNode follower0 = cluster.getAnyNodeExcept(leader);
        final ClusterNode follower1 = cluster.getAnyNodeExcept(leader, follower0);

        follower0.setClusterStateApplyResponse(ClusterStateApplyResponse.FAIL);
        AckCollector ackCollector = leader.submitRandomValue();
        cluster.stabilise(Cluster.DEFAULT_DELAY_VARIABILITY, 0L);
        assertTrue("expected ack from " + leader, ackCollector.hasAckedSuccessfully(leader));
        assertTrue("expected nack from " + follower0, ackCollector.hasAckedUnsuccessfully(follower0));
        assertTrue("expected ack from " + follower1, ackCollector.hasAckedSuccessfully(follower1));
        assertThat("leader should be last to ack", ackCollector.getSuccessfulAckIndex(leader), equalTo(1));
    }


    public void testAckListenerReceivesNackFromLeader() {
        final Cluster cluster = createStabilisedThreeNodeCluster();
        final ClusterNode leader = cluster.getAnyLeader();
        final ClusterNode follower0 = cluster.getAnyNodeExcept(leader);
        final ClusterNode follower1 = cluster.getAnyNodeExcept(leader, follower0);
        final long startingTerm = leader.legislator.getCurrentTerm();

        leader.setClusterStateApplyResponse(ClusterStateApplyResponse.FAIL);
        AckCollector ackCollector = leader.submitRandomValue();
        cluster.runUntil(cluster.currentTimeMillis + 3 * Cluster.DEFAULT_DELAY_VARIABILITY);
        assertTrue(leader.legislator.getMode() != Mode.LEADER || leader.legislator.getCurrentTerm() > startingTerm);
        leader.setClusterStateApplyResponse(ClusterStateApplyResponse.SUCCEED);
        cluster.stabilise();
        assertTrue("expected nack from " + leader, ackCollector.hasAckedUnsuccessfully(leader));
        assertTrue("expected ack from " + follower0, ackCollector.hasAckedSuccessfully(follower0));
        assertTrue("expected ack from " + follower1, ackCollector.hasAckedSuccessfully(follower1));
        assertTrue(leader.legislator.getMode() != Mode.LEADER || leader.legislator.getCurrentTerm() > startingTerm);
    }

    public void testAckListenerReceivesNoAckFromHangingFollower() {
        final Cluster cluster = createStabilisedThreeNodeCluster();
        final ClusterNode leader = cluster.getAnyLeader();
        final ClusterNode follower0 = cluster.getAnyNodeExcept(leader);
        final ClusterNode follower1 = cluster.getAnyNodeExcept(leader, follower0);

        follower0.setClusterStateApplyResponse(ClusterStateApplyResponse.HANG);
        AckCollector ackCollector = leader.submitRandomValue();
        cluster.runUntil(cluster.currentTimeMillis + 2 * Cluster.DEFAULT_DELAY_VARIABILITY);
        assertTrue("expected immediate ack from " + follower1, ackCollector.hasAckedSuccessfully(follower1));
        assertFalse("expected no ack from " + leader, ackCollector.hasAckedSuccessfully(leader));
        cluster.stabilise(cluster.DEFAULT_STABILISATION_TIME, 0L);
        assertTrue("expected eventual ack from " + leader, ackCollector.hasAckedSuccessfully(leader));
        assertFalse("expected no ack from " + follower0, ackCollector.hasAcked(follower0));
    }

    public void testAckListenerReceivesNacksIfPublicationTimesOut() {
        final Cluster cluster = createStabilisedThreeNodeCluster();
        final ClusterNode leader = cluster.getAnyLeader();
        final ClusterNode follower0 = cluster.getAnyNodeExcept(leader);
        final ClusterNode follower1 = cluster.getAnyNodeExcept(leader, follower0);

        follower0.isConnected = false;
        follower1.isConnected = false;
        AckCollector ackCollector = leader.submitRandomValue();
        cluster.runUntil(cluster.currentTimeMillis + Cluster.DEFAULT_DELAY_VARIABILITY);
        assertFalse("expected no immediate ack from " + leader, ackCollector.hasAcked(leader));
        assertFalse("expected no immediate ack from " + follower0, ackCollector.hasAcked(follower0));
        assertFalse("expected no immediate ack from " + follower1, ackCollector.hasAcked(follower1));
        follower0.isConnected = true;
        follower1.isConnected = true;
        cluster.stabilise(cluster.DEFAULT_STABILISATION_TIME, 0L);
        assertThat("leader should remain the same", cluster.getAnyLeader(), sameInstance(leader));
        assertTrue("expected eventual nack from " + follower0, ackCollector.hasAckedUnsuccessfully(follower0));
        assertTrue("expected eventual nack from " + follower1, ackCollector.hasAckedUnsuccessfully(follower1));
        assertTrue("expected eventual nack from " + leader, ackCollector.hasAckedUnsuccessfully(leader));
    }

    public void testAckListenerReceivesNacksIfLeaderStandsDown() {
        final Cluster cluster = createStabilisedThreeNodeCluster();
        final ClusterNode leader = cluster.getAnyLeader();
        final ClusterNode follower0 = cluster.getAnyNodeExcept(leader);
        final ClusterNode follower1 = cluster.getAnyNodeExcept(leader, follower0);

        leader.isConnected = false;
        follower0.legislator.handleDisconnectedNode(leader.localNode);
        follower1.legislator.handleDisconnectedNode(leader.localNode);
        cluster.runUntil(cluster.currentTimeMillis + cluster.DEFAULT_ELECTION_TIME);
        AckCollector ackCollector = leader.submitRandomValue();
        cluster.runUntil(cluster.currentTimeMillis + Cluster.DEFAULT_DELAY_VARIABILITY);
        leader.isConnected = true;
        cluster.stabilise(cluster.DEFAULT_STABILISATION_TIME, 0L);
        assertTrue("expected nack from " + leader, ackCollector.hasAckedUnsuccessfully(leader));
        assertTrue("expected nack from " + follower0, ackCollector.hasAckedUnsuccessfully(follower0));
        assertTrue("expected nack from " + follower1, ackCollector.hasAckedUnsuccessfully(follower1));
    }

    public void testAckListenerReceivesNacksFromFollowerInHigherTerm() {
        final Cluster cluster = createStabilisedThreeNodeCluster();
        final ClusterNode leader = cluster.getAnyLeader();
        final ClusterNode follower0 = cluster.getAnyNodeExcept(leader);
        final ClusterNode follower1 = cluster.getAnyNodeExcept(leader, follower0);

        follower0.legislator.handleStartJoin(new StartJoinRequest(follower0.localNode, follower0.legislator.getCurrentTerm() + 1));
        AckCollector ackCollector = leader.submitRandomValue();
        cluster.stabilise(cluster.DEFAULT_STABILISATION_TIME, 0L);
        assertTrue("expected ack from " + leader, ackCollector.hasAckedSuccessfully(leader));
        assertTrue("expected nack from " + follower0, ackCollector.hasAckedUnsuccessfully(follower0));
        assertTrue("expected ack from " + follower1, ackCollector.hasAckedSuccessfully(follower1));
    }

    // check that runRandomly leads to reproducible results
    public void testRepeatableTests() throws Exception {
        final Callable<Long> test = () -> {
            final Cluster cluster = new Cluster(randomIntBetween(1, 5));
            cluster.runRandomly(true);
            cluster.stabilise();
            return value(cluster.getAnyLeader().legislator.getLastAcceptedState());
        };
        final long seed = randomLong();
        logger.info("First run with seed [{}]", seed);
        final long result1 = RandomizedContext.current().runWithPrivateRandomness(seed, test);
        logger.info("Second run with seed [{}]", seed);
        final long result2 = RandomizedContext.current().runWithPrivateRandomness(seed, test);
        assertEquals(result1, result2);
    }

    static class AckCollector implements AckListener {

        private final Set<DiscoveryNode> ackedNodes = new HashSet<>();
        private final List<DiscoveryNode> successfulNodes = new ArrayList<>();
        private final List<DiscoveryNode> unsuccessfulNodes = new ArrayList<>();

        @Override
        public void onNodeAck(DiscoveryNode node, Exception e) {
            assertTrue("duplicate ack from " + node, ackedNodes.add(node));
            if (e == null) {
                successfulNodes.add(node);
            } else {
                unsuccessfulNodes.add(node);
            }
        }

        boolean hasAckedSuccessfully(ClusterNode clusterNode) {
            return successfulNodes.contains(clusterNode.localNode);
        }

        boolean hasAckedUnsuccessfully(ClusterNode clusterNode) {
            return unsuccessfulNodes.contains(clusterNode.localNode);
        }

        boolean hasAcked(ClusterNode clusterNode) {
            return ackedNodes.contains(clusterNode.localNode);
        }

        int getSuccessfulAckIndex(ClusterNode clusterNode) {
            assert successfulNodes.contains(clusterNode.localNode) : "get index of " + clusterNode;
            return successfulNodes.indexOf(clusterNode.localNode);
        }
    }

    /**
     * How to behave with a new cluster state
     */
    enum ClusterStateApplyResponse {
        /**
         * Apply the state (default)
         */
        SUCCEED,

        /**
         * Reject the state with an exception.
         */
        FAIL,

        /**
         * Never respond either way.
         */
        HANG,
    }

    private Cluster createStabilisedThreeNodeCluster() {
        final Cluster cluster = new Cluster(3);
        logger.info("--> createThreeNodeCluster: initial stabilisation");

        cluster.stabilise(cluster.DEFAULT_ELECTION_TIME, 0L);
        logger.info("--> createThreeNodeCluster: reconfigure so that all nodes have a vote");
        final VotingConfiguration allNodes = new VotingConfiguration(
            cluster.clusterNodes.stream().map(cn -> cn.localNode.getId()).collect(Collectors.toSet()));
        cluster.getAnyLeader().submitConfiguration(allNodes);
        cluster.stabilise(Cluster.DEFAULT_DELAY_VARIABILITY, 0L);
        cluster.runRandomly(false);
        cluster.stabilise();

        final ClusterNode leader = cluster.getAnyLeader();
        final ClusterNode follower0 = cluster.getAnyNodeExcept(leader);
        final ClusterNode follower1 = cluster.getAnyNodeExcept(leader, follower0);

        assertThat("leader should start out as LEADER", leader.legislator.getMode(), equalTo(Mode.LEADER));
        assertThat("follower0 should start out as FOLLOWER", follower0.legislator.getMode(), equalTo(Mode.FOLLOWER));
        assertThat("follower1 should start out as FOLLOWER", follower1.legislator.getMode(), equalTo(Mode.FOLLOWER));

        Matcher<Long> isCorrectVersion = is(leader.getLastAppliedClusterState().version());
        for (final ClusterNode clusterNode : cluster.clusterNodes) {
            assertThat(clusterNode.getLastAppliedClusterState().version(), isCorrectVersion);
            assertThat(clusterNode.legislator.getLastAcceptedState().version(), isCorrectVersion);
            assertThat(clusterNode.legislator.getLastCommittedState().get().version(), isCorrectVersion);
        }
        assertThat(leader.getLastAppliedClusterState().getLastAcceptedConfiguration().getNodeIds().size(), is(3));
        assertThat(leader.getLastAppliedClusterState().getLastCommittedConfiguration().getNodeIds().size(), is(3));

        logger.info("--> createThreeNodeCluster: end of setup");

        return cluster;
    }

    class Cluster {
        final List<ClusterNode> clusterNodes;
        private final List<InFlightMessage> inFlightMessages = new ArrayList<>();
        long currentTimeMillis = 0L;
        Map<Long, ClusterState> committedStatesByVersion = new HashMap<>();
        static final long DEFAULT_DELAY_VARIABILITY = 100L;
        static final long RANDOM_MODE_DELAY_VARIABILITY = 10000L;

        // How long does it take for the cluster to stabilise?

        // Each heartbeat takes at most this long:
        final long DEFAULT_MAX_HEARTBEAT_TIME
            = CONSENSUS_HEARTBEAT_DELAY_SETTING.get(Settings.EMPTY).millis()
            + CONSENSUS_HEARTBEAT_TIMEOUT_SETTING.get(Settings.EMPTY).millis()
            + 2 * DEFAULT_DELAY_VARIABILITY;
        // Multiple heartbeat failures are needed before the leader's failure is detected:
        final long DEFAULT_MAX_FAILURE_DETECTION_TIME
            = DEFAULT_MAX_HEARTBEAT_TIME * CONSENSUS_LEADER_CHECK_RETRY_COUNT_SETTING.get(Settings.EMPTY);
        // When stabilising, there are no election collisions, because we run to quiescence before waking any other nodes up.
        // Therefore elections takes this long:
        final long DEFAULT_ELECTION_TIME
            = 2 * (CONSENSUS_MIN_DELAY_SETTING.get(Settings.EMPTY).millis() + DEFAULT_DELAY_VARIABILITY);
        // Lag detection takes this long to notice that a follower is lagging the leader:
        final long DEFAULT_LAG_DETECTION_TIME
            = CONSENSUS_HEARTBEAT_DELAY_SETTING.get(Settings.EMPTY).millis() // before the heartbeat that hears about the lag
            + CONSENSUS_PUBLISH_TIMEOUT_SETTING.get(Settings.EMPTY).millis() // waiting for the lag
            + 2 * DEFAULT_DELAY_VARIABILITY;

        // Worst cases for stabilisation:
        //
        // 1. Just before stabilisation there was a leader which committed a value and then dropped off the network. All nodes must first
        // detect its failure and then elect a new one.
        //
        // 2. Just before stabilisation the leader published a value which wasn't received by one of the followers. The follower's lag
        // detection must notice this omission, then the master must notice the node is rejecting heartbeats and remove it from the cluster,
        // then the follower must notice it is missing from the cluster and rejoin.
        //
        final long DEFAULT_STABILISATION_TIME = RANDOM_MODE_DELAY_VARIABILITY
            + Math.max(DEFAULT_MAX_FAILURE_DETECTION_TIME + DEFAULT_ELECTION_TIME, // case 1
            DEFAULT_LAG_DETECTION_TIME + DEFAULT_MAX_FAILURE_DETECTION_TIME + DEFAULT_MAX_HEARTBEAT_TIME); // case 2

        Cluster(int nodeCount) {
            clusterNodes = new ArrayList<>(nodeCount);
            logger.info("--> creating cluster of {} nodes", nodeCount);

            for (int i = 0; i < nodeCount; i++) {
                clusterNodes.add(new ClusterNode(i));
            }

            final VotingConfiguration initialConfiguration = randomConfiguration();
            logger.info("--> initial configuration: {}", initialConfiguration);
            for (final ClusterNode clusterNode : clusterNodes) {
                clusterNode.initialise(initialConfiguration);
            }
        }

        void addNodes(int nodeCount) {
            int numCurrentNodes = clusterNodes.size();
            for (int i = 0; i < nodeCount; i++) {
                clusterNodes.add(new ClusterNode(numCurrentNodes + i).initialise(VotingConfiguration.EMPTY_CONFIG));
            }
        }

        int getNodeCount() {
            return clusterNodes.size();
        }

        void stabilise() {
            stabilise(DEFAULT_STABILISATION_TIME, DEFAULT_DELAY_VARIABILITY);
        }

        void stabilise(long stabilisationTimeMillis, long delayVariability) {
            // Stabilisation phase: just wake up nodes in order for long enough to allow a leader to be elected

            final long stabilisationPhaseEndMillis = currentTimeMillis + stabilisationTimeMillis;
            logger.info("--> starting stabilisation phase at [{}ms]: run for [{}ms] until [{}ms] with delayVariability [{}ms]",
                currentTimeMillis, stabilisationTimeMillis, stabilisationPhaseEndMillis, delayVariability);

            setDelayVariability(delayVariability);
            runUntil(stabilisationPhaseEndMillis);

            logger.info("--> end of stabilisation phase at [{}ms]", currentTimeMillis);

            assertUniqueLeaderAndExpectedModes();
        }

        void runUntil(long endTime) {
            deliverNextMessageUntilQuiescent();

            while (tasks.isEmpty() == false && getNextTaskExecutionTime() <= endTime) {
                doNextWakeUp();
                deliverNextMessageUntilQuiescent();
            }
        }

        /**
         * Assert that there is a unique leader node (in mode LEADER) and all other nodes are in mode FOLLOWER (if isConnected)
         * or CANDIDATE (if not isConnected). This is the expected steady state for a cluster in a network that's behaving normally.
         */
        private void assertUniqueLeaderAndExpectedModes() {
            final ClusterNode leader = getAnyLeader();
            Matcher<Optional<Long>> isPresentAndEqualToLeaderVersion
                = equalTo(Optional.of(leader.legislator.getLastAcceptedState().getVersion()));
            assertThat(leader.legislator.getLastCommittedState().map(ClusterState::getVersion), isPresentAndEqualToLeaderVersion);
            for (final ClusterNode clusterNode : clusterNodes) {
                if (clusterNode == leader) {
                    continue;
                }

                final String legislatorId = clusterNode.getId();

                if (clusterNode.isConnected == false) {
                    assertThat(legislatorId + " is a candidate", clusterNode.legislator.getMode(), is(Legislator.Mode.CANDIDATE));
                } else {
                    assertThat(legislatorId + " is a follower", clusterNode.legislator.getMode(), is(Legislator.Mode.FOLLOWER));
                    assertThat(legislatorId + " is at the same version as the leader",
                        Optional.of(clusterNode.legislator.getLastAcceptedState().getVersion()), isPresentAndEqualToLeaderVersion);
                    assertThat(legislatorId + " is at the same committed version as the leader",
                        clusterNode.legislator.getLastCommittedState().map(ClusterState::getVersion), isPresentAndEqualToLeaderVersion);
                }
            }
        }

        /**
         * @return Any node that is a LEADER. Throws an exception if there is no such node. It is up to the caller to
         * check the modes of all the other nodes to make sure, if expected, that there are no other LEADER nodes.
         */
        ClusterNode getAnyLeader() {
            List<ClusterNode> leaders = clusterNodes.stream()
                .filter(clusterNode -> clusterNode.legislator.getMode() == Legislator.Mode.LEADER)
                .collect(Collectors.toList());
            assertNotEquals("list of leaders should be nonempty", Collections.emptyList(), leaders);
            return randomFrom(leaders);
        }

        private final List<ClusterNode.TaskWithExecutionTime> tasks = new ArrayList<>();

        void runRandomly(boolean reconfigure) {
            // Safety phase: behave quite randomly and verify that there is no divergence, but without any expectation of progress.

            final int iterations = scaledRandomIntBetween(50, 10000);
            logger.info("--> start of safety phase of [{}] iterations", iterations);
            setDelayVariability(RANDOM_MODE_DELAY_VARIABILITY);

            for (int iteration = 0; iteration < iterations; iteration++) {
                try {
                    if (inFlightMessages.size() > 0 && usually()) {
                        deliverRandomMessage();
                    } else if (rarely()) {
                        // send a client value to a random node, preferring leaders
                        final ClusterNode clusterNode = randomLegislatorPreferringLeaders();
                        final int newValue = randomInt();
                        logger.debug("----> [safety {}] proposing new value [{}] to [{}]", iteration, newValue, clusterNode.getId());
                        clusterNode.submitValue(newValue);
                    } else if (reconfigure && rarely()) {
                        // perform a reconfiguration
                        final ClusterNode clusterNode = randomLegislatorPreferringLeaders();
                        final VotingConfiguration newConfig = randomConfiguration();
                        logger.debug("----> [safety {}] proposing reconfig [{}] to [{}]", iteration, newConfig, clusterNode.getId());
                        clusterNode.submitConfiguration(newConfig);
                    } else if (rarely()) {
                        // reboot random node
                        final ClusterNode clusterNode = randomLegislator();
                        logger.debug("----> [safety {}] rebooting [{}]", iteration, clusterNode.getId());
                        clusterNode.reboot();
                    } else if (rarely()) {
                        // abdicate leadership
                        final ClusterNode oldLeader = randomLegislatorPreferringLeaders();
                        final ClusterNode newLeader = randomLegislator();
                        logger.debug("----> [safety {}] [{}] abdicating to [{}]",
                            iteration, oldLeader.getId(), newLeader.getId());
                        oldLeader.legislator.abdicateTo(newLeader.localNode);
                    } else if (rarely()) {
                        // deal with an externally-detected failure
                        final ClusterNode clusterNode = randomLegislator();
                        logger.debug("----> [safety {}] failing [{}]", iteration, clusterNode.getId());
                        clusterNode.legislator.handleFailure();
                    } else if (tasks.isEmpty() == false) {
                        // execute next scheduled task
                        logger.debug("----> [safety {}] executing first task scheduled after time [{}ms]", iteration, currentTimeMillis);
                        doNextWakeUp();
                    }
                } catch (ConsensusMessageRejectedException ignored) {
                    // This is ok: it just means a message couldn't currently be handled.
                }

                assertConsistentStates();
            }

            setDelayVariability(DEFAULT_DELAY_VARIABILITY);
            logger.info("--> end of safety phase");
        }

        private void doNextWakeUp() {
            final long nextTaskExecutionTime = getNextTaskExecutionTime();

            assert nextTaskExecutionTime >= currentTimeMillis;
            logger.trace("----> advancing time by [{}ms] from [{}ms] to [{}ms]",
                nextTaskExecutionTime - currentTimeMillis, currentTimeMillis, nextTaskExecutionTime);
            currentTimeMillis = nextTaskExecutionTime;

            for (final ClusterNode.TaskWithExecutionTime task : tasks) {
                if (task.getExecutionTimeMillis() == nextTaskExecutionTime) {
                    tasks.remove(task);
                    logger.trace("----> executing {}", task);
                    task.run();
                    break; // in case there is more than one task with the same execution time
                }
            }
        }

        private Long getNextTaskExecutionTime() {
            return tasks.stream()
                .map(ClusterNode.TaskWithExecutionTime::getExecutionTimeMillis).min(Long::compareTo).get();
        }

        private long delayVariability = DEFAULT_DELAY_VARIABILITY;

        void sendFromTo(DiscoveryNode sender, DiscoveryNode destination, Consumer<Legislator> action) {
            final InFlightMessage inFlightMessage = new InFlightMessage(sender, destination, () -> {
                for (final ClusterNode clusterNode : clusterNodes) {
                    if (clusterNode.localNode.equals(destination) && clusterNode.isConnected) {
                        action.accept(clusterNode.legislator);
                    } else {
                        if (clusterNode.localNode.getId().equals(destination.getId()) &&
                            clusterNode.localNode.equals(destination) == false) {
                            logger.debug("found node with same id but different instance: {} instead of {}", clusterNode.localNode,
                                destination);
                        }
                    }
                }
            });

            if (inFlightMessage.isLocalAction() && randomBoolean()) {
                // TODO be more sensitive about which local actions are run inline and which are deferred
                inFlightMessage.run();
            } else {
                inFlightMessages.add(inFlightMessage);
            }
        }

        void sendPublishRequestFrom(DiscoveryNode sender, DiscoveryNode destination, PublishRequest publishRequest,
                                    TransportResponseHandler<LegislatorPublishResponse> responseHandler) {
            sendFromTo(sender, destination, e -> {
                try {
                    LegislatorPublishResponse publishResponse = e.handlePublishRequest(publishRequest);
                    sendFromTo(destination, sender, e2 -> responseHandler.handleResponse(publishResponse));
                } catch (Exception ex) {
                    sendFromTo(destination, sender, e2 -> responseHandler.handleException(new TransportException(ex)));
                }
            });
        }

        void sendHeartbeatRequestFrom(DiscoveryNode sender, DiscoveryNode destination, HeartbeatRequest heartbeatRequest,
                                      TransportResponseHandler<HeartbeatResponse> responseHandler) {
            sendFromTo(sender, destination, e -> {
                try {
                    HeartbeatResponse heartbeatResponse = e.handleHeartbeatRequest(sender, heartbeatRequest);
                    sendFromTo(destination, sender, e2 -> responseHandler.handleResponse(heartbeatResponse));
                } catch (Exception ex) {
                    sendFromTo(destination, sender, e2 -> responseHandler.handleException(new TransportException(ex)));
                }
            });
        }

        void sendApplyCommitFrom(DiscoveryNode sender, DiscoveryNode destination, ApplyCommit applyCommit,
                                 TransportResponseHandler<TransportResponse.Empty> responseHandler) {
            sendFromTo(sender, destination, e -> {
                try {
                    e.handleApplyCommit(sender, applyCommit, new ActionListener<Void>() {
                        @Override
                        public void onResponse(Void aVoid) {
                            sendFromTo(destination, sender, e2 -> responseHandler.handleResponse(TransportResponse.Empty.INSTANCE));
                        }

                        @Override
                        public void onFailure(Exception ex) {
                            sendFromTo(destination, sender, e2 -> responseHandler.handleException(new TransportException(ex)));
                        }
                    });
                } catch (Exception ex) {
                    sendFromTo(destination, sender, e2 -> responseHandler.handleException(new TransportException(ex)));
                }
            });
        }

        void sendSeekJoinsFrom(DiscoveryNode sender, DiscoveryNode destination, SeekJoins seekJoins,
                               TransportResponseHandler<OfferJoin> responseHandler) {
            sendFromTo(sender, destination, e -> {
                try {
                    OfferJoin offerJoin = e.handleSeekJoins(serialize(seekJoins, SeekJoins::new));
                    sendFromTo(destination, sender, e2 -> responseHandler.handleResponse(offerJoin));
                } catch (Exception ex) {
                    sendFromTo(destination, sender, e2 -> responseHandler.handleException(new TransportException(ex)));
                }
            });
        }

        void sendStartJoinFrom(DiscoveryNode sender, DiscoveryNode destination, StartJoinRequest startJoinRequest,
                               TransportResponseHandler<TransportResponse.Empty> responseHandler) {
            sendFromTo(sender, destination, e -> {
                try {
                    e.handleStartJoin(startJoinRequest);
                    sendFromTo(destination, sender, e2 -> responseHandler.handleResponse(TransportResponse.Empty.INSTANCE));
                } catch (Exception ex) {
                    sendFromTo(destination, sender, e2 -> responseHandler.handleException(new TransportException(ex)));
                }
            });
        }

        void sendJoinFrom(DiscoveryNode sender, DiscoveryNode destination, Join join,
                          TransportResponseHandler<TransportResponse.Empty> responseHandler) {
            sendFromTo(sender, destination, e -> {
                try {
                    e.handleJoinRequest(serialize(join, Join::new), new MembershipAction.JoinCallback() {
                        @Override
                        public void onSuccess() {
                            sendFromTo(destination, sender, e2 -> responseHandler.handleResponse(TransportResponse.Empty.INSTANCE));
                        }

                        @Override
                        public void onFailure(Exception e) {
                            sendFromTo(destination, sender, e2 -> responseHandler.handleException(new TransportException(e)));
                        }
                    });
                } catch (Exception ex) {
                    sendFromTo(destination, sender, e2 -> responseHandler.handleException(new TransportException(ex)));
                }
            });
        }

        void sendPreJoinHandoverFrom(DiscoveryNode sender, DiscoveryNode destination, PrejoinHandoverRequest prejoinHandoverRequest) {
            sendFromTo(sender, destination, e -> e.handlePreJoinHandover(prejoinHandoverRequest));
        }

        void sendAbdicationFrom(DiscoveryNode sender, DiscoveryNode destination, AbdicationRequest abdicationRequest) {
            sendFromTo(sender, destination, e -> e.handleAbdication(abdicationRequest));
        }

        void sendLeaderCheckRequestFrom(DiscoveryNode sender, DiscoveryNode destination,
                                        LeaderCheckRequest leaderCheckRequest,
                                        TransportResponseHandler<LeaderCheckResponse> responseHandler) {

            sendFromTo(sender, destination, e -> {
                try {
                    LeaderCheckResponse response = e.handleLeaderCheckRequest(leaderCheckRequest);
                    sendFromTo(destination, sender, e2 -> responseHandler.handleResponse(response));
                } catch (Exception ex) {
                    sendFromTo(destination, sender, e2 -> responseHandler.handleException(new TransportException(ex)));
                }
            });
        }

        private void assertConsistentStates() {
            for (final ClusterNode clusterNode : clusterNodes) {
                clusterNode.legislator.invariant();
            }
            updateCommittedStates();
        }

        private void updateCommittedStates() {
            for (final ClusterNode clusterNode : clusterNodes) {
                Optional<ClusterState> committedState = clusterNode.legislator.getLastCommittedState();
                if (committedState.isPresent()) {
                    ClusterState storedState = committedStatesByVersion.get(committedState.get().getVersion());
                    if (storedState == null) {
                        committedStatesByVersion.put(committedState.get().getVersion(), committedState.get());
                    } else {
                        assertEquals("expected " + committedState.get() + " but got " + storedState,
                            value(committedState.get()), value(storedState));
                    }
                }
            }
        }

        private void deliverNextMessage() {
            // TODO count message delays and assert bounds on this number
            inFlightMessages.remove(0).run();
        }

        void deliverNextMessageUntilQuiescent() {
            while (inFlightMessages.size() > 0) {
                try {
                    deliverNextMessage();
                } catch (ConsensusMessageRejectedException ignored) {
                    // This is ok: it just means a message couldn't currently be handled.
                }
                assertConsistentStates();
            }
        }

        private void deliverRandomMessage() {
            InFlightMessage action = inFlightMessages.remove(randomInt(inFlightMessages.size() - 1));
            if (usually() || action.isLocalAction()) {
                action.run();
            } else {
                logger.trace("dropping message from " + action.sender + " to " + action.destination);
            }
        }

        private VotingConfiguration randomConfiguration() {
            return new VotingConfiguration(randomSubsetOf(randomIntBetween(1, clusterNodes.size()), clusterNodes)
                .stream().map(cn -> cn.localNode.getId()).collect(Collectors.toSet()));
        }

        private ClusterNode randomLegislator() {
            return randomFrom(clusterNodes);
        }

        private ClusterNode randomLegislatorPreferringLeaders() {
            for (int i = 0; i < 3; i++) {
                ClusterNode clusterNode = randomLegislator();
                if (clusterNode.legislator.getMode() == Legislator.Mode.LEADER) {
                    return clusterNode;
                }
            }
            return randomLegislator();
        }

        void setDelayVariability(long delayVariability) {
            this.delayVariability = delayVariability;
        }

        ClusterNode getAnyNodeExcept(ClusterNode... avoidNodes) {
            List<ClusterNode> shuffledNodes = new ArrayList<>(clusterNodes);
            Collections.shuffle(shuffledNodes, random());
            for (final ClusterNode clusterNode : shuffledNodes) {
                if (Arrays.stream(avoidNodes).allMatch(avoidNode -> avoidNode != clusterNode)) {
                    return clusterNode;
                }
            }
            throw new AssertionError("no suitable node found");
        }

        class InFlightMessage implements Runnable {
            private final DiscoveryNode sender;
            private final DiscoveryNode destination;
            private final Runnable doDelivery;

            InFlightMessage(DiscoveryNode sender, DiscoveryNode destination, Runnable doDelivery) {
                this.sender = sender;
                this.destination = destination;
                this.doDelivery = doDelivery;
            }

            @Override
            public void run() {
                doDelivery.run();
            }

            boolean hasDestination(DiscoveryNode discoveryNode) {
                return destination.equals(discoveryNode);
            }

            boolean isLocalAction() {
                return sender.equals(destination);
            }
        }

        class ClusterNode {
            private final int index;
            private final MockTransport transport;
            private final FutureExecutor futureExecutor;

            PersistedState persistedState;
            Legislator legislator;
            boolean isConnected = true;
            DiscoveryNode localNode;
            FakeThreadPoolMasterService masterService;
            FakeClusterApplier clusterApplier;
            ClusterStateApplyResponse clusterStateApplyResponse = ClusterStateApplyResponse.SUCCEED;

            ClusterNode(int index) {
                this.index = index;
                this.futureExecutor = new FutureExecutor();
                localNode = createDiscoveryNode();
                transport = new MockTransport();
            }

            @Override
            public String toString() {
                return localNode.toString();
            }

            private DiscoveryNode createDiscoveryNode() {
                final TransportAddress transportAddress = buildNewFakeTransportAddress();
                // Generate the ephemeral ID deterministically, for repeatable tests. This means we have to pass everything else into
                // the constructor explicitly too.
                return new DiscoveryNode("", "node" + this.index, UUIDs.randomBase64UUID(random()),
                    transportAddress.address().getHostString(),
                    transportAddress.getAddress(), transportAddress, Collections.emptyMap(),
                    EnumSet.allOf(Role.class), Version.CURRENT);
            }

            private void setUpLegislatorAndMasterService() {
                FakeThreadPoolMasterService masterService = new FakeThreadPoolMasterService(
                    "fake threadpool for " + localNode, futureExecutor);
                Settings settings = Settings.builder()
                    .put("node.name", localNode.getId())
                    .build();
                clusterApplier = new FakeClusterApplier(settings);
                this.legislator = new Legislator(settings, persistedState, transport, masterService,
                    ESAllocationTestCase.createAllocationService(), localNode,
                    new CurrentTimeSupplier(), futureExecutor,
                    () -> clusterNodes.stream().filter(cn -> cn.isConnected || cn.getLocalNode().equals(localNode))
                        .map(ClusterNode::getLocalNode).collect(Collectors.toList()), clusterApplier, random());
                legislator.start();
                this.masterService = masterService;

                masterService.setClusterStatePublisher(legislator::publish);

                masterService.start();
                legislator.startInitialJoin();
            }

            public AckCollector submitUpdateTask(String source, UnaryOperator<ClusterState> clusterStateUpdate) {
                logger.trace("[{}] submitUpdateTask: enqueueing [{}]", localNode.getId(), source);
                final AckCollector ackCollector = new AckCollector();
                masterService.submitStateUpdateTask(source,
                    new ClusterStateUpdateTask() {
                        @Override
                        public ClusterState execute(ClusterState currentState) {
                            masterService.nextAckCollector = ackCollector;
                            return clusterStateUpdate.apply(currentState);
                        }

                        @Override
                        public void onFailure(String source, Exception e) {
                            logger.debug(() -> new ParameterizedMessage("failed to publish: [{}]", source), e);
                        }

                        @Override
                        public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                            updateCommittedStates();
                            ClusterState state = committedStatesByVersion.get(newState.version());
                            assertNotNull("State not committed : " + newState.toString(), state);
                            assertEquals(value(state), value(newState));
                            logger.trace("successfully published: [{}]", newState);
                        }
                    });
                return ackCollector;
            }

            ClusterNode initialise(VotingConfiguration initialConfiguration) {
                assert persistedState == null;
                assert legislator == null;
                persistedState = new BasePersistedState(0L,
                    clusterState(0L, 0L, localNode, initialConfiguration, initialConfiguration, 42L));
                setUpLegislatorAndMasterService();
                return this;
            }

            // TODO: have some tests that use the on-disk data for persistence across reboots.
            void reboot() {
                logger.trace("reboot: taking down [{}]", localNode);
                tasks.removeIf(task -> task.scheduledFor(localNode));
                inFlightMessages.removeIf(action -> action.hasDestination(localNode));
                localNode = createDiscoveryNode();
                logger.trace("reboot: starting up [{}]", localNode);
                try {
                    BytesStreamOutput outStream = new BytesStreamOutput();
                    outStream.setVersion(Version.CURRENT);
                    persistedState.getLastAcceptedState().writeTo(outStream);
                    StreamInput inStream = new NamedWriteableAwareStreamInput(outStream.bytes().streamInput(),
                        new NamedWriteableRegistry(ClusterModule.getNamedWriteables()));
                    persistedState.setLastAcceptedState(ClusterState.readFrom(inStream, localNode)); // adapts it to new localNode instance
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                setUpLegislatorAndMasterService();
            }

            String getId() {
                return localNode.getId();
            }

            public DiscoveryNode getLocalNode() {
                return localNode;
            }

            ClusterState getLastAppliedClusterState() {
                return clusterApplier.lastAppliedClusterState;
            }

            void setClusterStateApplyResponse(ClusterStateApplyResponse clusterStateApplyResponse) {
                this.clusterStateApplyResponse = clusterStateApplyResponse;
            }

            AckCollector submitConfiguration(VotingConfiguration config) {
                return submitUpdateTask("new config [" + config + "]",
                    state -> ClusterState.builder(state).lastAcceptedConfiguration(config).build());
            }

            AckCollector submitValue(long value) {
                return submitUpdateTask("new value [" + value + "]", state -> setValue(state, value));
            }

            AckCollector submitRandomValue() {
                return submitValue(randomLong());
            }

            private class FakeClusterApplier implements ClusterApplier {

                final ClusterName clusterName;
                ClusterState lastAppliedClusterState;

                private FakeClusterApplier(Settings settings) {
                    clusterName = ClusterName.CLUSTER_NAME_SETTING.get(settings);
                }

                @Override
                public void setInitialState(ClusterState initialState) {
                    assert lastAppliedClusterState == null;
                    assert initialState != null;
                    lastAppliedClusterState = initialState;
                }

                @Override
                public void onNewClusterState(String source, Supplier<ClusterState> clusterStateSupplier, ClusterApplyListener listener) {
                    switch (clusterStateApplyResponse) {
                        case SUCCEED:
                            futureExecutor.schedule(TimeValue.ZERO, "apply cluster state from [" + source + "]", () -> {
                                final ClusterState oldClusterState = clusterApplier.lastAppliedClusterState;
                                final ClusterState newClusterState = clusterStateSupplier.get();
                                assert oldClusterState.version() <= newClusterState.version() : "updating cluster state from version "
                                    + oldClusterState.version() + " to stale version " + newClusterState.version();
                                clusterApplier.lastAppliedClusterState = newClusterState;
                                listener.onSuccess(source);
                            });
                            break;
                        case FAIL:
                            futureExecutor.schedule(TimeValue.ZERO, "fail to apply cluster state from [" + source + "]",
                                () -> listener.onFailure(source, new ElasticsearchException("cluster state application failed")));
                            break;
                        case HANG:
                            if (randomBoolean()) {
                                futureExecutor.schedule(TimeValue.ZERO, "apply cluster state from [" + source + "] without ack", () -> {
                                    final ClusterState oldClusterState = clusterApplier.lastAppliedClusterState;
                                    final ClusterState newClusterState = clusterStateSupplier.get();
                                    assert oldClusterState.version() <= newClusterState.version() : "updating cluster state from version "
                                        + oldClusterState.version() + " to stale version " + newClusterState.version();
                                    clusterApplier.lastAppliedClusterState = newClusterState;
                                });
                            }
                            break;
                    }
                }

                @Override
                public Builder newClusterStateBuilder() {
                    return ClusterState.builder(clusterName);
                }
            }

            private class FutureExecutor implements Legislator.FutureExecutor {

                @Override
                public void schedule(TimeValue delay, String description, Runnable task) {
                    assert delay.getMillis() >= 0;
                    final long actualDelay = delay.getMillis() + randomLongBetween(0L, delayVariability);
                    final long executionTimeMillis = currentTimeMillis + actualDelay;
                    logger.debug("[{}] scheduling [{}]: requested delay [{}ms] after [{}ms], " +
                            "scheduling with delay [{}ms] at [{}ms]",
                        localNode.getId(), description, delay.getMillis(), currentTimeMillis, actualDelay, executionTimeMillis);
                    tasks.add(new TaskWithExecutionTime(executionTimeMillis, task, localNode, description));
                }
            }

            private class MockTransport implements Transport {

                @Override
                public void sendPublishRequest(DiscoveryNode destination, PublishRequest publishRequest,
                                               TransportResponseHandler<LegislatorPublishResponse> responseHandler) {
                    if (isConnected) {
                        sendPublishRequestFrom(localNode, destination, publishRequest, responseHandler);
                    }
                }

                @Override
                public void sendHeartbeatRequest(DiscoveryNode destination, HeartbeatRequest heartbeatRequest,
                                                 TransportResponseHandler<HeartbeatResponse> responseHandler) {
                    if (isConnected) {
                        sendHeartbeatRequestFrom(localNode, destination, heartbeatRequest, responseHandler);
                    }
                }

                @Override
                public void sendApplyCommit(DiscoveryNode destination, ApplyCommit applyCommit,
                                            TransportResponseHandler<TransportResponse.Empty> responseHandler) {
                    if (isConnected) {
                        sendApplyCommitFrom(localNode, destination, applyCommit, responseHandler);
                    }
                }

                @Override
                public void sendSeekJoins(DiscoveryNode destination, SeekJoins seekJoins,
                                          TransportResponseHandler<OfferJoin> responseHandler) {
                    if (isConnected) {
                        sendSeekJoinsFrom(localNode, destination, seekJoins, responseHandler);
                    }
                }

                @Override
                public void sendStartJoin(DiscoveryNode destination, StartJoinRequest startJoinRequest,
                                          TransportResponseHandler<TransportResponse.Empty> responseHandler) {
                    if (isConnected) {
                        sendStartJoinFrom(localNode, destination, startJoinRequest, responseHandler);
                    }
                }

                @Override
                public void sendJoin(DiscoveryNode destination, Join join,
                                     TransportResponseHandler<TransportResponse.Empty> responseHandler) {
                    if (isConnected) {
                        sendJoinFrom(localNode, destination, join, responseHandler);
                    }
                }

                @Override
                public void sendPreJoinHandover(DiscoveryNode destination, PrejoinHandoverRequest prejoinHandoverRequest) {
                    if (isConnected) {
                        sendPreJoinHandoverFrom(localNode, destination, prejoinHandoverRequest);
                    }
                }

                @Override
                public void sendAbdication(DiscoveryNode destination, AbdicationRequest abdicationRequest) {
                    if (isConnected) {
                        sendAbdicationFrom(localNode, destination, abdicationRequest);
                    }
                }

                @Override
                public void sendLeaderCheckRequest(DiscoveryNode destination, LeaderCheckRequest leaderCheckRequest,
                                                   TransportResponseHandler<LeaderCheckResponse> responseHandler) {
                    if (isConnected) {
                        sendLeaderCheckRequestFrom(localNode, destination, leaderCheckRequest, responseHandler);
                    }
                }
            }

            private class TaskWithExecutionTime implements Runnable {
                final long executionTimeMillis;
                final Runnable task;
                private final DiscoveryNode taskNode;
                private final String description;

                TaskWithExecutionTime(long executionTimeMillis, Runnable task, DiscoveryNode taskNode, String description) {
                    this.executionTimeMillis = executionTimeMillis;
                    this.task = task;
                    this.taskNode = taskNode;
                    this.description = description;
                }

                long getExecutionTimeMillis() {
                    return executionTimeMillis;
                }

                @Override
                public String toString() {
                    return "[" + description + "] on [" + taskNode + "] scheduled at time [" + executionTimeMillis + "ms]";
                }

                boolean scheduledFor(DiscoveryNode discoveryNode) {
                    return taskNode.equals(discoveryNode);
                }

                @Override
                public void run() {
                    task.run();
                }
            }
        }

        private class CurrentTimeSupplier implements LongSupplier {
            @Override
            public long getAsLong() {
                return currentTimeMillis;
            }
        }

    }

    static class FakeThreadPoolMasterService extends MasterService {

        private final String name;
        private final List<Runnable> tasks = new ArrayList<>();
        private final ClusterNode.FutureExecutor futureExecutor;
        private boolean scheduledNextTask = false;
        private boolean taskInProgress = false;
        private boolean waitForPublish = false;
        AckCollector nextAckCollector = new AckCollector();

        FakeThreadPoolMasterService(String name, ClusterNode.FutureExecutor futureExecutor) {
            super(Settings.EMPTY, null);
            this.name = name;
            this.futureExecutor = futureExecutor;
        }

        @Override
        protected PrioritizedEsThreadPoolExecutor createThreadPoolExecutor() {
            return new PrioritizedEsThreadPoolExecutor(name, 1, 1, 1, TimeUnit.SECONDS, EsExecutors.daemonThreadFactory(name),
                null, null) {

                @Override
                public void execute(Runnable command, final TimeValue timeout, final Runnable timeoutCallback) {
                    futureExecutor.schedule(timeout, command.toString(), () -> execute(command));
                }

                @Override
                public void execute(Runnable command) {
                    tasks.add(command);
                    scheduleNextTaskIfNecessary();
                }
            };
        }

        private void scheduleNextTaskIfNecessary() {
            if (taskInProgress == false && tasks.isEmpty() == false && scheduledNextTask == false) {
                scheduledNextTask = true;
                final Runnable firstTask = tasks.get(0);
                futureExecutor.schedule(TimeValue.ZERO, "next master service task", () -> {
                    assert taskInProgress == false;
                    assert waitForPublish == false;
                    assert scheduledNextTask;
                    final Runnable task = tasks.remove(0);
                    assertThat(firstTask, equalTo(task));
                    taskInProgress = true;
                    scheduledNextTask = false;
                    task.run();
                    if (waitForPublish == false) {
                        taskInProgress = false;
                    }
                    scheduleNextTaskIfNecessary();
                });
            }
        }

        @Override
        protected Builder incrementVersion(ClusterState clusterState) {
            // generate cluster UUID deterministically for repeatable tests
            return ClusterState.builder(clusterState).incrementVersion().stateUUID(UUIDs.randomBase64UUID(random()));
        }

        @Override
        protected void publish(ClusterChangedEvent clusterChangedEvent, TaskOutputs taskOutputs, long startTimeNS) {
            assert waitForPublish == false;
            waitForPublish = true;
            final AckCollector ackCollector = nextAckCollector;
            nextAckCollector = new AckCollector();
            final AckListener ackListener = taskOutputs.createAckListener(threadPool, clusterChangedEvent.state());
            final ActionListener<Void> publishListener = getPublishListener(clusterChangedEvent, taskOutputs, startTimeNS);
            clusterStatePublisher.publish(clusterChangedEvent, new ActionListener<Void>() {

                    private boolean listenerCalled = false;

                    @Override
                    public void onResponse(Void aVoid) {
                        assert listenerCalled == false;
                        listenerCalled = true;
                        assert waitForPublish;
                        waitForPublish = false;
                        try {
                            publishListener.onResponse(null);
                        } finally {
                            taskInProgress = false;
                            scheduleNextTaskIfNecessary();
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        assertThat(e.getMessage(), not(containsString("is in progress")));
                        assert listenerCalled == false;
                        listenerCalled = true;
                        assert waitForPublish;
                        waitForPublish = false;
                        try {
                            publishListener.onFailure(e);
                        } finally {
                            taskInProgress = false;
                            scheduleNextTaskIfNecessary();
                        }
                    }
                },
                (node, e) -> {
                    ackCollector.onNodeAck(node, e);
                    ackListener.onNodeAck(node, e);
                });
        }
    }

    // TODO: remove this once we use proper transport layer with serialization or once we've fixed NodeJoinController / TaskBatching
    public <T extends Writeable> T serialize(T t, Writeable.Reader<T> reader) {
        try {
            BytesStreamOutput outStream = new BytesStreamOutput();
            t.writeTo(outStream);
            StreamInput inStream = outStream.bytes().streamInput();
            return reader.read(inStream);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
