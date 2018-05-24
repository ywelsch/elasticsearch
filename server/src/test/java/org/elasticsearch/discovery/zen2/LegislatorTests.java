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

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterState.Builder;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.ClusterState.VotingConfiguration;
import org.elasticsearch.cluster.service.ClusterApplier;
import org.elasticsearch.cluster.node.DiscoveryNode.Role;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.discovery.zen.MembershipAction;
import org.elasticsearch.discovery.zen2.ConsensusState.BasePersistedState;
import org.elasticsearch.discovery.zen2.ConsensusState.PersistedState;
import org.elasticsearch.discovery.zen2.Legislator.Mode;
import org.elasticsearch.discovery.zen2.Legislator.Transport;
import org.elasticsearch.discovery.zen2.LegislatorTests.Cluster.ClusterNode;
import org.elasticsearch.discovery.zen2.Messages.ApplyCommit;
import org.elasticsearch.discovery.zen2.Messages.HeartbeatRequest;
import org.elasticsearch.discovery.zen2.Messages.HeartbeatResponse;
import org.elasticsearch.discovery.zen2.Messages.LeaderCheckResponse;
import org.elasticsearch.discovery.zen2.Messages.LegislatorPublishResponse;
import org.elasticsearch.discovery.zen2.Messages.OfferJoin;
import org.elasticsearch.discovery.zen2.Messages.PublishRequest;
import org.elasticsearch.discovery.zen2.Messages.SeekJoins;
import org.elasticsearch.discovery.zen2.Messages.StartJoinRequest;
import org.elasticsearch.discovery.zen2.Messages.Join;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static org.elasticsearch.discovery.zen2.Legislator.CONSENSUS_HEARTBEAT_DELAY_SETTING;
import static org.elasticsearch.discovery.zen2.Legislator.CONSENSUS_HEARTBEAT_TIMEOUT_SETTING;
import static org.elasticsearch.discovery.zen2.Legislator.CONSENSUS_LEADER_CHECK_RETRY_COUNT_SETTING;
import static org.elasticsearch.discovery.zen2.Legislator.CONSENSUS_MIN_DELAY_SETTING;
import static org.elasticsearch.discovery.zen2.Legislator.CONSENSUS_PUBLISH_TIMEOUT_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

public class LegislatorTests extends ESTestCase {

    public void testCanProposeValueAfterStabilisation() {
        Cluster cluster = new Cluster(randomIntBetween(1, 5));
        cluster.runRandomly(true);
        cluster.stabilise();
        final ClusterNode leader = cluster.getAnyLeader();
        final long stabilisedVersion = leader.legislator.getLastCommittedState().get().getVersion();

        final long finalValue = randomInt();
        logger.info("--> proposing final value [{}] to [{}]", finalValue, leader.getId());
        leader.handleClientValue(ConsensusStateTests.nextStateWithValue(leader.legislator.getLastAcceptedState(), finalValue));
        cluster.deliverNextMessageUntilQuiescent();

        for (final ClusterNode clusterNode : cluster.clusterNodes) {
            final String legislatorId = clusterNode.getId();
            final ClusterState committedState = clusterNode.legislator.getLastCommittedState().get();
            assertThat(legislatorId + " is at the next version", committedState.getVersion(), equalTo(stabilisedVersion + 1));
            assertThat(legislatorId + " has the right value", ConsensusStateTests.value(committedState), is(finalValue));
        }

        cluster.stabilise(Cluster.DEFAULT_DELAY_VARIABILITY, 0L);
        for (final ClusterNode clusterNode : cluster.clusterNodes) {
            final String legislatorId = clusterNode.getId();
            final ClusterState appliedState = clusterNode.getLastAppliedClusterState();
            assertThat(legislatorId + " has applied the latest cluster state", appliedState.getVersion(), equalTo(stabilisedVersion + 1));
        }
    }

    public void testNodeJoiningAndLeaving() {
        Cluster cluster = new Cluster(randomIntBetween(1, 3));
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
        ClusterNode leader = cluster.getAnyLeader();

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
        Cluster cluster = new Cluster(randomIntBetween(1, 5));
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
        Cluster cluster = new Cluster(3);
        cluster.runRandomly(true);
        cluster.stabilise();
        ClusterNode leader = cluster.getAnyLeader();

        final VotingConfiguration allNodes = new VotingConfiguration(
            cluster.clusterNodes.stream().map(cn -> cn.localNode.getId()).collect(Collectors.toSet()));

        // TODO: have the following automatically done as part of a reconfiguration subsystem
        if (leader.legislator.hasElectionQuorum(allNodes) == false) {
            logger.info("--> leader does not have a join quorum for the new configuration, abdicating to self");
            // abdicate to self to acquire all join votes
            leader.legislator.abdicateTo(leader.localNode);

            cluster.stabilise();
            leader = cluster.getAnyLeader();
        }

        leader.handleClientValue(ConsensusStateTests.nextStateWithConfig(leader.legislator.getLastAcceptedState(), allNodes));
        cluster.stabilise(Cluster.DEFAULT_DELAY_VARIABILITY, 0L);

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
        Cluster cluster = new Cluster(3);
        cluster.runRandomly(true);
        cluster.stabilise();
        ClusterNode leader = cluster.getAnyLeader();

        final VotingConfiguration allNodes = new VotingConfiguration(
            cluster.clusterNodes.stream().map(cn -> cn.localNode.getId()).collect(Collectors.toSet()));

        // TODO: have the following automatically done as part of a reconfiguration subsystem
        if (leader.legislator.hasElectionQuorum(allNodes) == false) {
            logger.info("--> leader does not have a join quorum for the new configuration, abdicating to self");
            // abdicate to self to acquire all join votes
            leader.legislator.abdicateTo(leader.localNode);

            cluster.stabilise();
            leader = cluster.getAnyLeader();
        }

        logger.info("--> start of reconfiguration to make all nodes into voting nodes");

        leader.handleClientValue(ConsensusStateTests.nextStateWithConfig(leader.legislator.getLastAcceptedState(), allNodes));
        cluster.stabilise(Cluster.DEFAULT_DELAY_VARIABILITY, 0L);

        logger.info("--> end of reconfiguration to make all nodes into voting nodes");

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

        cluster.stabilise(CONSENSUS_MIN_DELAY_SETTING.get(Settings.EMPTY).millis() * 2 + Cluster.DEFAULT_DELAY_VARIABILITY,
            Cluster.DEFAULT_DELAY_VARIABILITY);

        // Furthermore the first one to wake up causes an election to complete successfully, because we run to quiescence
        // before waking any other nodes up. Therefore the cluster has a unique leader and all connected nodes are FOLLOWERs.
        cluster.assertConsistentStates();
        cluster.assertUniqueLeaderAndExpectedModes();
    }

    public void testFastFailureWhenAQuorumReboots() {
        Cluster cluster = new Cluster(3);
        cluster.runRandomly(true);
        cluster.stabilise();
        ClusterNode leader = cluster.getAnyLeader();

        final VotingConfiguration allNodes = new VotingConfiguration(
            cluster.clusterNodes.stream().map(cn -> cn.localNode.getId()).collect(Collectors.toSet()));

        // TODO: have the following automatically done as part of a reconfiguration subsystem
        if (leader.legislator.hasElectionQuorum(allNodes) == false) {
            logger.info("--> leader does not have a join quorum for the new configuration, abdicating to self");
            // abdicate to self to acquire all join votes
            leader.legislator.abdicateTo(leader.localNode);

            cluster.stabilise();
            leader = cluster.getAnyLeader();
        }

        logger.info("--> start of reconfiguration to make all nodes into voting nodes");

        leader.handleClientValue(ConsensusStateTests.nextStateWithConfig(leader.legislator.getLastAcceptedState(), allNodes));
        cluster.stabilise(Cluster.DEFAULT_DELAY_VARIABILITY, 0L);
        cluster.assertConsistentStates();

        logger.info("--> end of reconfiguration to make all nodes into voting nodes");

        for (final ClusterNode clusterNode : cluster.clusterNodes) {
            if (clusterNode.equals(leader) == false) {
                clusterNode.isConnected = false;
            }
        }

        logger.info("--> nodes disconnected - starting publication");

        leader.handleClientValue(ConsensusStateTests.nextStateWithValue(leader.legislator.getLastAcceptedState(), randomInt()));

        logger.info("--> publication started - now rebooting nodes");

        for (final ClusterNode clusterNode : cluster.clusterNodes) {
            if (clusterNode.equals(leader) == false) {
                DiscoveryNode oldLocalNode = clusterNode.localNode;
                clusterNode.reboot();
                clusterNode.isConnected = true;
                leader.legislator.handleDisconnectedNode(oldLocalNode);
            }
        }

        logger.info("--> nodes rebooted - now stabilising");

        cluster.stabilise(CONSENSUS_MIN_DELAY_SETTING.get(Settings.EMPTY).millis() + Cluster.DEFAULT_DELAY_VARIABILITY, 0L);

        cluster.assertConsistentStates();

        for (final ClusterNode clusterNode : cluster.clusterNodes) {
            assertTrue(clusterNode.localNode + " is in committed cluster state",
                leader.legislator.getLastCommittedState().get().getNodes().nodeExists(clusterNode.localNode));
        }

        // TODO want to assert that we stabilised quickly by failing the earlier publication but cannot since overlapping publications
        // are currently allowed. But this will assert that in future.
    }

    public void testFastRemovalWhenFollowerDropsConnections() {
        Cluster cluster = new Cluster(3);
        cluster.runRandomly(true);
        cluster.stabilise();
        ClusterNode leader = cluster.getAnyLeader();

        final VotingConfiguration allNodes = new VotingConfiguration(
            cluster.clusterNodes.stream().map(cn -> cn.localNode.getId()).collect(Collectors.toSet()));

        // TODO: have the following automatically done as part of a reconfiguration subsystem
        if (leader.legislator.hasElectionQuorum(allNodes) == false) {
            logger.info("--> leader does not have a join quorum for the new configuration, abdicating to self");
            // abdicate to self to acquire all join votes
            leader.legislator.abdicateTo(leader.localNode);

            cluster.stabilise();
            leader = cluster.getAnyLeader();
        }

        logger.info("--> start of reconfiguration to make all nodes into voting nodes");

        leader.handleClientValue(ConsensusStateTests.nextStateWithConfig(leader.legislator.getLastAcceptedState(), allNodes));
        cluster.stabilise(Cluster.DEFAULT_DELAY_VARIABILITY, 0L);

        logger.info("--> end of reconfiguration to make all nodes into voting nodes");

        ClusterNode nodeToDisconnect;
        do {
            nodeToDisconnect = cluster.randomLegislator();
        } while (nodeToDisconnect.legislator.getMode() == Mode.LEADER);

        assertThat(leader.getLastAppliedClusterState().nodes().get(nodeToDisconnect.localNode.getId()), not(nullValue()));
        long lastAppliedVersion = leader.getLastAppliedClusterState().version();

        nodeToDisconnect.isConnected = false;

        logger.info("--> notifying {} of node disconnection", leader.getLocalNode());
        leader.legislator.handleDisconnectedNode(nodeToDisconnect.localNode);

        logger.info("--> failing {}", nodeToDisconnect.getLocalNode());
        nodeToDisconnect.legislator.handleFailure();

        cluster.stabilise(Cluster.DEFAULT_DELAY_VARIABILITY, 0L);

        assertThat(leader.getLastAppliedClusterState().nodes().get(nodeToDisconnect.localNode.getId()), nullValue());
        assertThat(leader.getLastAppliedClusterState().version(), is(lastAppliedVersion + 1));
    }

    public void testLagDetectionCausesRejoin() {
        Cluster cluster = new Cluster(3);
        cluster.runRandomly(true);
        cluster.stabilise();
        ClusterNode leader = cluster.getAnyLeader();

        final VotingConfiguration allNodes = new VotingConfiguration(
            cluster.clusterNodes.stream().map(cn -> cn.localNode.getId()).collect(Collectors.toSet()));

        // TODO: have the following automatically done as part of a reconfiguration subsystem
        if (leader.legislator.hasElectionQuorum(allNodes) == false) {
            logger.info("--> leader does not have a join quorum for the new configuration, abdicating to self");
            // abdicate to self to acquire all join votes
            leader.legislator.abdicateTo(leader.localNode);

            cluster.stabilise();
            leader = cluster.getAnyLeader();
        }

        logger.info("--> start of reconfiguration to make all nodes into voting nodes");

        leader.handleClientValue(ConsensusStateTests.nextStateWithConfig(leader.legislator.getLastAcceptedState(), allNodes));
        cluster.stabilise(Cluster.DEFAULT_DELAY_VARIABILITY, 0L);

        logger.info("--> end of reconfiguration to make all nodes into voting nodes");

        for (final ClusterNode clusterNode : cluster.clusterNodes) {
            if (clusterNode != leader) {
                logger.info("--> disconnecting {}", clusterNode.getLocalNode());
                clusterNode.isConnected = false;
                break;
            }
        }

        leader.handleClientValue(ConsensusStateTests.nextStateWithValue(leader.legislator.getLastAcceptedState(), randomLong()));
        cluster.deliverNextMessageUntilQuiescent();

        for (final ClusterNode clusterNode : cluster.clusterNodes) {
            logger.info("--> reconnecting {}", clusterNode.getLocalNode());
            clusterNode.isConnected = true;
        }

        cluster.stabilise();

        // Furthermore the first one to wake up causes an election to complete successfully, because we run to quiescence
        // before waking any other nodes up. Therefore the cluster has a unique leader and all connected nodes are FOLLOWERs.
        cluster.assertConsistentStates();
        cluster.assertUniqueLeaderAndExpectedModes();
    }

    class Cluster {
        private final List<ClusterNode> clusterNodes;
        private final List<InFlightMessage> inFlightMessages = new ArrayList<>();
        private long currentTimeMillis = 0L;
        Map<Long, ClusterState> committedStatesByVersion = new HashMap<>();
        private static final long DEFAULT_DELAY_VARIABILITY = 100L;
        private static final long RANDOM_MODE_DELAY_VARIABILITY = 10000L;
        private long masterServicesTaskId = 0L;

        // How long does it take for the cluster to stabilise?

        // Each heartbeat takes at most this long:
        private final long DEFAULT_MAX_HEARTBEAT_TIME
            = CONSENSUS_HEARTBEAT_DELAY_SETTING.get(Settings.EMPTY).millis()
            + CONSENSUS_HEARTBEAT_TIMEOUT_SETTING.get(Settings.EMPTY).millis()
            + 2 * DEFAULT_DELAY_VARIABILITY;
        // Multiple heartbeat failures are needed before the leader's failure is detected:
        private final long DEFAULT_MAX_FAILURE_DETECTION_TIME
            = DEFAULT_MAX_HEARTBEAT_TIME * CONSENSUS_LEADER_CHECK_RETRY_COUNT_SETTING.get(Settings.EMPTY);
        // When stabilising, there are no election collisions, because we run to quiescence before waking any other nodes up.
        // Therefore elections takes this long:
        private final long DEFAULT_ELECTION_TIME
            = 2 * (CONSENSUS_MIN_DELAY_SETTING.get(Settings.EMPTY).millis() + DEFAULT_DELAY_VARIABILITY);
        // Lag detection takes this long to notice that a follower is lagging the leader:
        private final long DEFAULT_LAG_DETECTION_TIME
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
        private final long DEFAULT_STABILISATION_TIME = RANDOM_MODE_DELAY_VARIABILITY
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

        public void addNodes(int nodeCount) {
            int numCurrentNodes = clusterNodes.size();
            for (int i = 0; i < nodeCount; i++) {
                clusterNodes.add(new ClusterNode(numCurrentNodes + i).initialise(VotingConfiguration.EMPTY_CONFIG));
            }
        }

        public int getNodeCount() {
            return clusterNodes.size();
        }

        public void stabilise() {
            stabilise(DEFAULT_STABILISATION_TIME, DEFAULT_DELAY_VARIABILITY);
        }

        public void stabilise(long stabilisationTimeMillis, long delayVariability) {
            // Stabilisation phase: just wake up nodes in order for long enough to allow a leader to be elected

            final long stabilisationPhaseEndMillis = currentTimeMillis + stabilisationTimeMillis;
            logger.info("--> starting stabilisation phase at [{}ms]: run for [{}ms] until [{}ms] with delayVariability [{}ms]",
                currentTimeMillis, stabilisationTimeMillis, stabilisationPhaseEndMillis, delayVariability);

            setDelayVariability(delayVariability);

            deliverNextMessageUntilQuiescent();

            while (tasks.isEmpty() == false && getNextTaskExecutionTime() <= stabilisationPhaseEndMillis) {
                doNextWakeUp();
                deliverNextMessageUntilQuiescent();
            }

            logger.info("--> end of stabilisation phase at [{}ms]", currentTimeMillis);

            assertUniqueLeaderAndExpectedModes();
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
        private ClusterNode getAnyLeader() {
            List<ClusterNode> leaders = clusterNodes.stream()
                .filter(clusterNode -> clusterNode.legislator.getMode() == Legislator.Mode.LEADER)
                .collect(Collectors.toList());
            assertNotEquals("list of leaders should be nonempty", Collections.emptyList(), leaders);
            return randomFrom(leaders);
        }

        private final List<ClusterNode.TaskWithExecutionTime> tasks = new ArrayList<>();

        private void runRandomly(boolean reconfigure) {
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
                        clusterNode.handleClientValue(
                            ConsensusStateTests.nextStateWithValue(clusterNode.legislator.getLastAcceptedState(), newValue));
                    } else if (reconfigure && rarely()) {
                        // perform a reconfiguration
                        final ClusterNode clusterNode = randomLegislatorPreferringLeaders();
                        final VotingConfiguration newConfig = randomConfiguration();
                        logger.debug("----> [safety {}] proposing reconfig [{}] to [{}]", iteration, newConfig, clusterNode.getId());
                        clusterNode.handleClientValue(
                            ConsensusStateTests.nextStateWithConfig(clusterNode.legislator.getLastAcceptedState(), newConfig));
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
                    LegislatorPublishResponse publishResponse = e.handlePublishRequest(sender, publishRequest);
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
                    OfferJoin offerJoin = e.handleSeekJoins(sender, seekJoins);
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
                    e.handleStartJoin(sender, startJoinRequest);
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
                    e.handleJoinRequest(sender, join, new MembershipAction.JoinCallback() {
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

        void sendPreJoinHandoverFrom(DiscoveryNode sender, DiscoveryNode destination) {
            sendFromTo(sender, destination, e -> e.handlePreJoinHandover(sender));
        }

        void sendAbdicationFrom(DiscoveryNode sender, DiscoveryNode destination, long currentTerm) {
            sendFromTo(sender, destination, e -> e.handleAbdication(sender, currentTerm));
        }

        void sendLeaderCheckRequestFrom(DiscoveryNode sender, DiscoveryNode destination,
                                        TransportResponseHandler<LeaderCheckResponse> responseHandler) {

            sendFromTo(sender, destination, e -> {
                try {
                    LeaderCheckResponse response = e.handleLeaderCheckRequest(sender);
                    sendFromTo(destination, sender, e2 -> responseHandler.handleResponse(response));
                } catch (Exception ex) {
                    sendFromTo(destination, sender, e2 -> responseHandler.handleException(new TransportException(ex)));
                }
            });
        }

        private void assertConsistentStates() {
            for (final ClusterNode clusterNode : clusterNodes) {
                Optional<ClusterState> committedState = clusterNode.legislator.getLastCommittedState();
                if (committedState.isPresent()) {
                    ClusterState storedState = committedStatesByVersion.get(committedState.get().getVersion());
                    if (storedState == null) {
                        committedStatesByVersion.put(committedState.get().getVersion(), committedState.get());
                    } else {
                        assertEquals("expected " + committedState.get() + " but got " + storedState,
                            ConsensusStateTests.value(committedState.get()), ConsensusStateTests.value(storedState));
                    }
                }
                clusterNode.legislator.invariant();
            }
        }

        private void deliverNextMessage() {
            // TODO count message delays and assert bounds on this number
            inFlightMessages.remove(0).run();
        }

        private void deliverNextMessageUntilQuiescent() {
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

        public void setDelayVariability(long delayVariability) {
            this.delayVariability = delayVariability;
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
            private final List<PendingTask> pendingTasks = new ArrayList<>();

            PersistedState persistedState;
            Legislator legislator;
            boolean isConnected = true;
            DiscoveryNode localNode;
            FakeClusterApplier clusterApplier;

            ClusterNode(int index) {
                this.index = index;
                this.futureExecutor = new FutureExecutor();
                localNode = createDiscoveryNode();
                transport = new MockTransport();
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

            public void handleClientValue(ClusterState clusterState) {
                legislator.handleClientValue(clusterState, new ActionListener<Void>() {
                    @Override
                    public void onResponse(Void aVoid) {
                        scheduleRunPendingTasks();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        scheduleRunPendingTasks();
                    }

                    private void scheduleRunPendingTasks() {
                        futureExecutor.schedule(TimeValue.ZERO, "scheduleRunPendingTasks", ClusterNode.this::runPendingTasks);
                    }
                });
            }

            private void runPendingTasks() {
                // TODO this batches tasks more aggressively than the real MasterService does, which weakens the properties we are
                // testing here. Make the batching more realistic.
                List<PendingTask> currentPendingTasks = new ArrayList<>(pendingTasks);
                pendingTasks.clear();
                if (currentPendingTasks.size() == 0) {
                    return;
                }

                try {
                    if (legislator.getMode() == Legislator.Mode.LEADER) {
                        ClusterState newState = legislator.getLastAcceptedState();
                        for (final PendingTask pendingTask : currentPendingTasks) {
                            logger.trace("[{}] running [{}]", localNode.getId(), pendingTask);
                            newState = pendingTask.run(newState);
                        }
                        assert Objects.equals(newState.getNodes().getMasterNodeId(), localNode.getId()) : newState;
                        assert newState.getNodes().getNodes().get(localNode.getId()) != null;
                        handleClientValue(ClusterState.builder(newState).term(legislator.getCurrentTerm()).incrementVersion()
                            // incrementVersion() gives the new state a random UUID, but we override this for test repeatability:
                            .stateUUID(UUIDs.randomBase64UUID(random())).build());
                    }
                } catch (ConsensusMessageRejectedException e) {
                    logger.trace(() -> new ParameterizedMessage("[{}] runPendingTasks: failed, rescheduling: {}",
                        localNode.getId(), e.getMessage()));
                    pendingTasks.addAll(currentPendingTasks);
                }
            }

            private void sendMasterServiceTask(String reason, UnaryOperator<ClusterState> task) {
                PendingTask pendingTask = new PendingTask(task, reason, masterServicesTaskId++);
                logger.trace("[{}] sendMasterServiceTask: enqueueing [{}]", localNode.getId(), pendingTask);
                pendingTasks.add(pendingTask);
                futureExecutor.schedule(TimeValue.ZERO, "sendMasterServiceTask: " + pendingTask, this::runPendingTasks);
            }

            ClusterNode initialise(VotingConfiguration initialConfiguration) {
                assert persistedState == null;
                assert legislator == null;
                persistedState = new BasePersistedState(0L,
                    ConsensusStateTests.clusterState(0L, 0L, localNode, initialConfiguration, initialConfiguration, 42L));
                createNewLegislator();
                return this;
            }

            // TODO: have some tests that use the on-disk data for persistence across reboots.
            void reboot() {
                logger.trace("reboot: taking down [{}]", localNode);
                tasks.removeIf(task -> task.scheduledFor(localNode));
                inFlightMessages.removeIf(action -> action.hasDestination(localNode));
                pendingTasks.clear();
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
                createNewLegislator();
            }

            private void createNewLegislator() {
                Settings settings = Settings.builder()
                    .put("node.name", localNode.getId())
                    .build();
                clusterApplier = new FakeClusterApplier(settings);
                legislator = new Legislator(settings, persistedState, transport, this::sendMasterServiceTask, localNode,
                    new CurrentTimeSupplier(), futureExecutor,
                    () -> clusterNodes.stream().filter(cn -> cn.isConnected || cn.getLocalNode().equals(localNode))
                        .map(ClusterNode::getLocalNode).collect(Collectors.toList()), clusterApplier);
            }

            String getId() {
                return localNode.getId();
            }

            public DiscoveryNode getLocalNode() {
                return localNode;
            }

            public ClusterState getLastAppliedClusterState() {
                return clusterApplier.lastAppliedClusterState;
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
                    futureExecutor.schedule(TimeValue.ZERO, "apply cluster state from [" + source + "]", () -> {
                        final ClusterState oldClusterState = lastAppliedClusterState;
                        final ClusterState newClusterState = clusterStateSupplier.get();
                        assert oldClusterState.version() <= newClusterState.version() : "updating cluster state from version "
                            + oldClusterState.version() + " to stale version " + newClusterState.version();
                        lastAppliedClusterState = newClusterState;
                        listener.onSuccess(source);
                    });
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

            private class PendingTask {
                private final UnaryOperator<ClusterState> task;
                private final String description;
                private final long taskId;

                PendingTask(UnaryOperator<ClusterState> task, String description, long taskId) {
                    this.task = task;
                    this.description = description;
                    this.taskId = taskId;
                }

                @Override
                public String toString() {
                    return description + " [" + taskId + "]";
                }

                ClusterState run(ClusterState clusterState) {
                    return task.apply(clusterState);
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
                public void sendPreJoinHandover(DiscoveryNode destination) {
                    if (isConnected) {
                        sendPreJoinHandoverFrom(localNode, destination);
                    }
                }

                @Override
                public void sendAbdication(DiscoveryNode destination, long currentTerm) {
                    if (isConnected) {
                        sendAbdicationFrom(localNode, destination, currentTerm);
                    }
                }

                @Override
                public void sendLeaderCheckRequest(DiscoveryNode destination,
                                                   TransportResponseHandler<LeaderCheckResponse> responseHandler) {
                    if (isConnected) {
                        sendLeaderCheckRequestFrom(localNode, destination, responseHandler);
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

                public long getExecutionTimeMillis() {
                    return executionTimeMillis;
                }

                @Override
                public String toString() {
                    return "[" + description + "] on [" + taskNode + "] scheduled at time [" + executionTimeMillis + "ms]";
                }

                public boolean scheduledFor(DiscoveryNode discoveryNode) {
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
}
