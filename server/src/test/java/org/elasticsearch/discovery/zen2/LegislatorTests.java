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
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.ClusterState.VotingConfiguration;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.discovery.zen.MembershipAction;
import org.elasticsearch.discovery.zen2.ConsensusState.BasePersistedState;
import org.elasticsearch.discovery.zen2.ConsensusState.PersistedState;
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
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import static org.elasticsearch.discovery.zen2.Legislator.CONSENSUS_HEARTBEAT_DELAY_SETTING;
import static org.elasticsearch.discovery.zen2.Legislator.CONSENSUS_HEARTBEAT_TIMEOUT_SETTING;
import static org.elasticsearch.discovery.zen2.Legislator.CONSENSUS_LEADER_CHECK_RETRY_COUNT_SETTING;
import static org.elasticsearch.discovery.zen2.Legislator.CONSENSUS_MIN_DELAY_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Is.is;

public class LegislatorTests extends ESTestCase {

    @TestLogging("org.elasticsearch.discovery.zen2:TRACE")
    public void testCanProposeValueAfterStabilisation() {
        Cluster cluster = new Cluster(randomIntBetween(1, 5));
        cluster.runRandomly(true);
        cluster.stabilise();
        final ClusterNode leader = cluster.getAnyLeader();
        final long stabilisedVersion = leader.legislator.getLastCommittedState().get().getVersion();

        final long finalValue = randomInt();
        logger.info("--> proposing final value [{}] to [{}]", finalValue, leader.getId());
        leader.legislator.handleClientValue(ConsensusStateTests.nextStateWithValue(leader.legislator.getLastAcceptedState(), finalValue));
        cluster.deliverNextMessageUntilQuiescent();

        for (final ClusterNode clusterNode : cluster.clusterNodes) {
            final String legislatorId = clusterNode.getId();
            final ClusterState committedState = clusterNode.legislator.getLastCommittedState().get();
            assertThat(legislatorId + " is at the next version", committedState.getVersion(), equalTo(stabilisedVersion + 1));
            assertThat(legislatorId + " has the right value", ConsensusStateTests.value(committedState), is(finalValue));
        }
    }

    @TestLogging("org.elasticsearch.discovery.zen2:TRACE")
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

    @TestLogging("org.elasticsearch.discovery.zen2:TRACE")
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

    @TestLogging("org.elasticsearch.discovery.zen2:TRACE")
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

        leader.legislator.handleClientValue(ConsensusStateTests.nextStateWithConfig(leader.legislator.getLastAcceptedState(), allNodes));
        cluster.deliverNextMessageUntilQuiescent();

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

    @TestLogging("org.elasticsearch.discovery.zen2:TRACE")
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

        leader.legislator.handleClientValue(ConsensusStateTests.nextStateWithConfig(leader.legislator.getLastAcceptedState(), allNodes));
        cluster.deliverNextMessageUntilQuiescent();

        logger.info("--> end of reconfiguration to make all nodes into voting nodes");

        final long disconnectionTime = cluster.currentTimeMillis;
        leader.isConnected = false;

        for (ClusterNode clusterNode : cluster.clusterNodes) {
            if (clusterNode != leader) {
                logger.info("--> notifying {} of leader disconnection", clusterNode.getLocalNode());
                clusterNode.legislator.handleDisconnectedNode(leader.localNode);
            }
        }

        // The nodes all entered mode CANDIDATE, so the next wake-up should be after a delay of at most 2 * CONSENSUS_MIN_DELAY_SETTING.
        final long stabilisationTime = disconnectionTime + CONSENSUS_MIN_DELAY_SETTING.get(Settings.EMPTY).millis() * 2
            + Cluster.DEFAULT_DELAY_VARIABILITY;
        logger.info("--> performing wake-ups until [{}ms]", stabilisationTime);
        while (cluster.tasks.isEmpty() == false && cluster.getNextTaskExecutionTime() < stabilisationTime) {
            cluster.doNextWakeUp();
            cluster.deliverNextMessageUntilQuiescent();
        }
        logger.info("--> next wake-ups completed");

        logger.info("--> failing old leader");
        leader.legislator.handleFailure();
        logger.info("--> finished failing old leader");

        // Furthermore the first one to wake up causes an election to complete successfully, because we run to quiescence
        // before waking any other nodes up. Therefore the cluster has a unique leader and all connected nodes are FOLLOWERs.
        cluster.assertConsistentStates();
        cluster.assertUniqueLeaderAndExpectedModes();
    }

    @TestLogging("org.elasticsearch.discovery.zen2:TRACE")
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

        leader.legislator.handleClientValue(ConsensusStateTests.nextStateWithConfig(leader.legislator.getLastAcceptedState(), allNodes));
        cluster.deliverNextMessageUntilQuiescent();

        logger.info("--> end of reconfiguration to make all nodes into voting nodes");

        final long disconnectionTime = cluster.currentTimeMillis;
        leader.isConnected = false;

        for (ClusterNode clusterNode : cluster.clusterNodes) {
            if (clusterNode != leader) {
                logger.info("--> notifying {} of leader disconnection", clusterNode.getLocalNode());
                clusterNode.legislator.handleDisconnectedNode(leader.localNode);
            }
        }

        // The nodes all entered mode CANDIDATE, so the next wake-up should be after a delay of at most 2 * CONSENSUS_MIN_DELAY_SETTING.
        final long stabilisationTime = disconnectionTime + CONSENSUS_MIN_DELAY_SETTING.get(Settings.EMPTY).millis() * 2
            + Cluster.DEFAULT_DELAY_VARIABILITY;
        logger.info("--> performing wake-ups until [{}ms]", stabilisationTime);
        while (cluster.tasks.isEmpty() == false && cluster.getNextTaskExecutionTime() < stabilisationTime) {
            cluster.doNextWakeUp();
            cluster.deliverNextMessageUntilQuiescent();
        }
        logger.info("--> next wake-ups completed");

        logger.info("--> failing old leader");
        leader.legislator.handleFailure();
        logger.info("--> finished failing old leader");

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

        // How long to wait? The worst case is that a leader just committed a value to all the other nodes, and then
        // dropped off the network, which would mean that all the other nodes must detect its failure. It takes
        // CONSENSUS_LEADER_CHECK_RETRY_COUNT_SETTING consecutive leader checks to fail before a follower becomes a
        // candidates, and with an unresponsive leader each leader check takes up to
        // CONSENSUS_HEARTBEAT_DELAY_SETTING + CONSENSUS_HEARTBEAT_TIMEOUT_SETTING. After all the retries have
        // failed, nodes wake up, become candidates, and wait for up to 2 * CONSENSUS_MIN_DELAY_SETTING before
        // attempting an election. The first election is expected to succeed, however, because we run to quiescence
        // before waking any other nodes up.
        private final long DEFAULT_STABILISATION_TIME =
            (CONSENSUS_HEARTBEAT_DELAY_SETTING.get(Settings.EMPTY).millis() +
                CONSENSUS_HEARTBEAT_TIMEOUT_SETTING.get(Settings.EMPTY).millis()) *
                CONSENSUS_LEADER_CHECK_RETRY_COUNT_SETTING.get(Settings.EMPTY) +
                2 * CONSENSUS_MIN_DELAY_SETTING.get(Settings.EMPTY).millis() +
                RANDOM_MODE_DELAY_VARIABILITY + DEFAULT_DELAY_VARIABILITY;

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
            setDelayVariability(delayVariability);

            deliverNextMessageUntilQuiescent();

            final long stabilisationPhaseEndMillis = currentTimeMillis + stabilisationTimeMillis;
            logger.info("--> start of stabilisation phase ({}ms): run until time {}ms", stabilisationTimeMillis,
                stabilisationPhaseEndMillis);

            while (tasks.isEmpty() == false && getNextTaskExecutionTime() <= stabilisationPhaseEndMillis) {
                doNextWakeUp();
                deliverNextMessageUntilQuiescent();
            }

            logger.info("--> end of stabilisation phase");

            assertUniqueLeaderAndExpectedModes();
        }

        /**
         * Assert that there is a unique leader node (in mode LEADER) and all other nodes are in mode FOLLOWER (if isConnected)
         * or CANDIDATE (if not isConnected). This is the expected steady state for a cluster in a network that's behaving normally.
         */
        private void assertUniqueLeaderAndExpectedModes() {
            final ClusterNode leader = getAnyLeader();
            final Matcher<Long> isSameAsLeaderVersion = is(leader.legislator.getLastAcceptedState().getVersion());
            assertTrue(leader.getId() + " has no committed state", leader.legislator.getLastCommittedState().isPresent());
            assertThat(leader.legislator.getLastCommittedState().get().getVersion(), isSameAsLeaderVersion);
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
                        clusterNode.legislator.getLastAcceptedState().getVersion(), isSameAsLeaderVersion);
                    assertThat(legislatorId + " is at the same committed version as the leader",
                        clusterNode.legislator.getLastCommittedState().get().getVersion(), isSameAsLeaderVersion);
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
            logger.info("--> start of safety phase");
            setDelayVariability(RANDOM_MODE_DELAY_VARIABILITY);

            for (int iteration = 0; iteration < 10000; iteration++) {
                try {
                    if (inFlightMessages.size() > 0 && usually()) {
                        deliverRandomMessage();
                    } else if (rarely()) {
                        // send a client value to a random node, preferring leaders
                        final ClusterNode clusterNode = randomLegislatorPreferringLeaders();
                        final int newValue = randomInt();
                        logger.info("----> [safety {}] proposing new value [{}] to [{}]", iteration, newValue, clusterNode.getId());
                        clusterNode.legislator.handleClientValue(
                            ConsensusStateTests.nextStateWithValue(clusterNode.legislator.getLastAcceptedState(), newValue));
                    } else if (reconfigure && rarely()) {
                        // perform a reconfiguration
                        final ClusterNode clusterNode = randomLegislatorPreferringLeaders();
                        final VotingConfiguration newConfig = randomConfiguration();
                        logger.info("----> [safety {}] proposing reconfig [{}] to [{}]", iteration, newConfig, clusterNode.getId());
                        clusterNode.legislator.handleClientValue(
                            ConsensusStateTests.nextStateWithConfig(clusterNode.legislator.getLastAcceptedState(), newConfig));
                    } else if (rarely()) {
                        // reboot random node
                        final ClusterNode clusterNode = randomLegislator();
                        logger.info("----> [safety {}] rebooting [{}]", iteration, clusterNode.getId());
                        clusterNode.reboot();
                    } else if (rarely()) {
                        // abdicate leadership
                        final ClusterNode oldLeader = randomLegislatorPreferringLeaders();
                        final ClusterNode newLeader = randomLegislator();
                        logger.info("----> [safety {}] [{}] abdicating to [{}]",
                            iteration, oldLeader.getId(), newLeader.getId());
                        oldLeader.legislator.abdicateTo(newLeader.localNode);
                    } else if (rarely()) {
                        // deal with an externally-detected failure
                        final ClusterNode clusterNode = randomLegislator();
                        logger.info("----> [safety {}] failing [{}]", iteration, clusterNode.getId());
                        clusterNode.legislator.handleFailure();
                    } else if (tasks.isEmpty() == false) {
                        // execute next scheduled task
                        logger.info("----> [safety {}] executing first task scheduled after time [{}ms]", iteration, currentTimeMillis);
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
            logger.info("----> advancing time by [{}ms] from [{}ms] to [{}ms]",
                nextTaskExecutionTime - currentTimeMillis, currentTimeMillis, nextTaskExecutionTime);
            currentTimeMillis = nextTaskExecutionTime;

            for (final ClusterNode.TaskWithExecutionTime task : tasks) {
                if (task.getExecutionTimeMillis() == nextTaskExecutionTime) {
                    tasks.remove(task);
                    logger.info("----> executing {}", task);
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
                            logger.debug("found node with same id but different instance: {} instead of  {}", clusterNode.localNode,
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
                    e.handleApplyCommit(sender, applyCommit);
                    sendFromTo(destination, sender, e2 -> responseHandler.handleResponse(TransportResponse.Empty.INSTANCE));
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

            PersistedState persistedState;
            Legislator legislator;
            boolean isConnected = true;
            DiscoveryNode localNode;

            ClusterNode(int index) {
                this.index = index;
                this.futureExecutor = new FutureExecutor();
                localNode = createDiscoveryNode();
                transport = new MockTransport();
            }

            private DiscoveryNode createDiscoveryNode() {
                return new DiscoveryNode("node" + this.index, buildNewFakeTransportAddress(), Version.CURRENT);
            }

            private void sendMasterServiceTask(String reason, Function<ClusterState, ClusterState> runnable) {
                futureExecutor.schedule(TimeValue.timeValueMillis(0L), () -> {
                    try {
                        if (legislator.getMode() == Legislator.Mode.LEADER) {
                            ClusterState newState = runnable.apply(legislator.getLastAcceptedState());
                            assert Objects.equals(newState.getNodes().getMasterNodeId(), localNode.getId()) : newState;
                            assert newState.getNodes().getNodes().get(localNode.getId()) != null;
                            legislator.handleClientValue(
                                ClusterState.builder(newState).term(legislator.getCurrentTerm()).incrementVersion().build());
                        }
                    } catch (ConsensusMessageRejectedException ignore) {
                        logger.trace(() -> new ParameterizedMessage("[{}] sendMasterServiceTask: [{}] failed: {}",
                            localNode.getName(), reason, ignore.getMessage()));
                        sendMasterServiceTask(reason, runnable);
                    }
                });
            }

            ClusterNode initialise(VotingConfiguration initialConfiguration) {
                assert persistedState == null;
                assert legislator == null;
                persistedState = new BasePersistedState(0L,
                    ConsensusStateTests.clusterState(0L, 0L, localNode, initialConfiguration, initialConfiguration, 42L));
                legislator = createLegislator();
                return this;
            }

            // TODO: have some tests that use the on-disk data for persistence across reboots.
            void reboot() {
                tasks.removeIf(task -> task.scheduledFor(localNode));
                inFlightMessages.removeIf(action -> action.hasDestination(localNode));
                localNode = createDiscoveryNode();
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
                legislator = createLegislator();
            }

            private Legislator createLegislator() {
                Settings settings = Settings.builder()
                    .put("node.name", localNode.getId())
                    .build();
                return new Legislator(settings, persistedState, transport, this::sendMasterServiceTask, localNode,
                    new CurrentTimeSupplier(), futureExecutor,
                    () -> clusterNodes.stream().filter(cn -> cn.isConnected || cn.getLocalNode().equals(localNode))
                        .map(ClusterNode::getLocalNode).collect(Collectors.toList()));
            }

            String getId() {
                return localNode.getId();
            }

            public DiscoveryNode getLocalNode() {
                return localNode;
            }

            private class FutureExecutor implements Legislator.FutureExecutor {

                @Override
                public void schedule(TimeValue delay, Runnable task) {
                    assert delay.getMillis() >= 0;
                    final long actualDelay = delay.getMillis() + randomLongBetween(0L, delayVariability);
                    final long executionTimeMillis = currentTimeMillis + actualDelay;
                    logger.debug("[{}] schedule: requested delay [{}ms] after [{}ms], " +
                            "scheduling with delay [{}ms] at [{}ms]",
                        localNode.getId(), delay.getMillis(), currentTimeMillis, actualDelay, executionTimeMillis);
                    tasks.add(new TaskWithExecutionTime(executionTimeMillis, task, localNode));
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

                TaskWithExecutionTime(long executionTimeMillis, Runnable task, DiscoveryNode taskNode) {
                    this.executionTimeMillis = executionTimeMillis;
                    this.task = task;
                    this.taskNode = taskNode;
                }

                public long getExecutionTimeMillis() {
                    return executionTimeMillis;
                }

                @Override
                public String toString() {
                    return "task on [" + taskNode + "] scheduled at time [" + executionTimeMillis + "ms]";
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
