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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.zen2.ConsensusState.AcceptedState;
import org.elasticsearch.discovery.zen2.ConsensusState.NodeCollection;
import org.elasticsearch.discovery.zen2.ConsensusState.Persistence;
import org.elasticsearch.discovery.zen2.ConsensusStateTests.ClusterState;
import org.elasticsearch.discovery.zen2.Legislator.Transport;
import org.elasticsearch.discovery.zen2.LegislatorTests.Cluster.ClusterNode;
import org.elasticsearch.discovery.zen2.Messages.ApplyCommit;
import org.elasticsearch.discovery.zen2.Messages.OfferVote;
import org.elasticsearch.discovery.zen2.Messages.PublishRequest;
import org.elasticsearch.discovery.zen2.Messages.PublishResponse;
import org.elasticsearch.discovery.zen2.Messages.SeekVotes;
import org.elasticsearch.discovery.zen2.Messages.Vote;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

import static org.elasticsearch.discovery.zen2.Legislator.CONSENSUS_COMMIT_DELAY_SETTING;
import static org.elasticsearch.discovery.zen2.Legislator.CONSENSUS_MIN_DELAY_SETTING;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.core.Is.is;

public class LegislatorTests extends ESTestCase {

    @TestLogging("org.elasticsearch.discovery.zen2:TRACE")
    public void testCanProposeValueAfterStabilisation() {
        Cluster cluster = new Cluster(randomIntBetween(1, 5));
        cluster.runRandomly(true);
        cluster.stabilise();
        final ClusterNode leader = cluster.getAnyLeader();
        final long stabilisedSlot = leader.legislator.getCommittedState().getSlot();

        final int finalValue = randomInt();
        logger.info("--> proposing final value [{}] to [{}]", finalValue, leader.getId());
        leader.legislator.handleClientValue(ConsensusStateTests.createUpdate("set final value " + finalValue,
            cs -> new ClusterState(cs.getSlot() + 1, cs.getVotingNodes(), finalValue)));
        cluster.deliverNextMessageUntilQuiescent();

        for (final ClusterNode clusterNode : cluster.clusterNodes) {
            final String legislatorId = clusterNode.getId();
            final ClusterState committedState = clusterNode.legislator.getCommittedState();
            assertThat(legislatorId + " is at the next slot", committedState.getSlot(), is(stabilisedSlot + 1));
            assertThat(legislatorId + " has the right value", committedState.getValue(), is(finalValue));
        }
    }

    @TestLogging("org.elasticsearch.discovery.zen2:TRACE")
    public void testCanAbdicateAfterStabilisation() {
        Cluster cluster = new Cluster(randomIntBetween(1, 5));
        cluster.runRandomly(true);
        cluster.stabilise();
        final ClusterNode leader = cluster.getAnyLeader();
        final long stabilisedSlot = leader.legislator.getCommittedState().getSlot();

        final ClusterNode newLeader = cluster.randomLegislator();
        logger.info("--> abdicating from [{}] to [{}]", leader.getId(), newLeader.getId());
        leader.legislator.abdicateTo(newLeader.localNode);
        cluster.deliverNextMessageUntilQuiescent();

        for (final ClusterNode clusterNode : cluster.clusterNodes) {
            final String legislatorId = clusterNode.getId();
            final Legislator<ClusterState> legislator = clusterNode.legislator;
            assertThat(legislatorId + " is at the next slot", legislator.getCommittedState().getSlot(), is(stabilisedSlot + 1));
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
        final ClusterNode leader = cluster.getAnyLeader();

        final NodeCollection allNodes = new NodeCollection();
        for (ClusterNode clusterNode : cluster.clusterNodes) {
            allNodes.add(clusterNode.localNode);
        }

        leader.legislator.handleClientValue(ConsensusStateTests.createUpdate("set configuration to " + allNodes,
            cs -> new ClusterState(cs.getSlot() + 1, allNodes, cs.getValue())));
        cluster.deliverNextMessageUntilQuiescent();

        leader.isConnected = false;
        cluster.runRandomly(false);
        cluster.stabilise();
        final ClusterNode newLeader = cluster.getAnyLeader();

        assertNotEquals(leader, newLeader);
        assertTrue(newLeader.isConnected);
    }

    @TestLogging("org.elasticsearch.discovery.zen2:TRACE")
    public void testFastElectionWhenLeaderDropsConnections() {
        Cluster cluster = new Cluster(3);
        cluster.runRandomly(true);
        cluster.stabilise();
        final ClusterNode leader = cluster.getAnyLeader();

        final NodeCollection allNodes = new NodeCollection();
        for (ClusterNode clusterNode : cluster.clusterNodes) {
            allNodes.add(clusterNode.localNode);
        }

        leader.legislator.handleClientValue(ConsensusStateTests.createUpdate("set configuration to " + allNodes,
            cs -> new ClusterState(cs.getSlot() + 1, allNodes, cs.getValue())));
        cluster.deliverNextMessageUntilQuiescent();

        final long disconnectionTime = cluster.currentTimeMillis;
        leader.isConnected = false;
        leader.legislator.ignoreWakeUpsForAtLeast(TimeValue.timeValueSeconds(10));

        for (ClusterNode clusterNode : cluster.clusterNodes) {
            if (clusterNode != leader) {
                clusterNode.legislator.handleDisconnectedNode(leader.localNode);
            }
        }
        cluster.doNextWakeUp();

        // The nodes all entered mode CANDIDATE, so the next WakeUp was after a delay of at most 2 * CONSENSUS_MIN_DELAY_SETTING.
        final long minDelayMillis = CONSENSUS_MIN_DELAY_SETTING.get(Settings.EMPTY).millis();
        assertThat("re-election time is short", cluster.currentTimeMillis - disconnectionTime, lessThan(minDelayMillis * 2));

        // Furthermore the first one to wake up causes an election to complete successfully, because we run to quiescence
        // before waking any other nodes up. Therefore the cluster has a unique leader and all connected nodes are FOLLOWERs.
        cluster.assertConsistentStates();
    }

    class Cluster {
        private final List<ClusterNode> clusterNodes;
        private final List<Runnable> pendingActions = new ArrayList<>();
        private long currentTimeMillis = 0L;

        Cluster(int nodeCount) {
            clusterNodes = new ArrayList<>(nodeCount);
            for (int i = 0; i < nodeCount; i++) {
                clusterNodes.add(new ClusterNode(i));
            }

            final NodeCollection initialConfiguration = randomConfiguration();
            for (final ClusterNode clusterNode : clusterNodes) {
                clusterNode.initialise(initialConfiguration);
            }
        }

        private void stabilise() {
            // Stabilisation phase: just wake up nodes in order for long enough to allow a leader to be elected
            deliverNextMessageUntilQuiescent();

            // How long to wait? The worst case is that a leader just committed a value to all the other nodes, and then
            // dropped off the network, which would mean that they all wait for up to CONSENSUS_COMMIT_DELAY_SETTING
            // before waking up again. Then they wake up, become candidates, and wait for up to
            // 2 * CONSENSUS_MIN_DELAY_SETTING before attempting an election. The first election is expected to succeed, however,
            // because we run to quiescence before waking any other nodes up.
            final long catchUpPhaseEndMillis = currentTimeMillis +
                CONSENSUS_COMMIT_DELAY_SETTING.get(Settings.EMPTY).millis() +
                CONSENSUS_MIN_DELAY_SETTING.get(Settings.EMPTY).millis() * 2;

            logger.info("--> start of stabilisation phase: run until time {}ms", catchUpPhaseEndMillis);

            while (currentTimeMillis < catchUpPhaseEndMillis) {
                doNextWakeUp();
                deliverNextMessageUntilQuiescent();
            }

            assertUniqueLeaderAndExpectedModes();
            logger.info("--> end of stabilisation phase");
        }

        /**
         * Assert that there is a unique leader node (in mode LEADER/INCUMBENT) and all other nodes are in mode FOLLOWER (if isConnected)
         * or CANDIDATE (if not isConnected). This is the expected steady state for a cluster in a network that's behaving normally.
         */
        private void assertUniqueLeaderAndExpectedModes() {
            final ClusterNode leader = getAnyLeader();
            final Matcher<Long> isSameAsLeaderSlot = is(leader.legislator.getCommittedState().getSlot());
            for (final ClusterNode clusterNode : clusterNodes) {
                if (clusterNode == leader) {
                    continue;
                }

                final String legislatorId = clusterNode.getId();

                if (clusterNode.isConnected == false) {
                    assertThat(legislatorId + " is a candidate", clusterNode.legislator.getMode(), is(Legislator.Mode.CANDIDATE));
                } else {
                    assertThat(legislatorId + " is a follower", clusterNode.legislator.getMode(), is(Legislator.Mode.FOLLOWER));
                    assertThat(legislatorId + " is at the same slot as the leader",
                        clusterNode.legislator.getCommittedState().getSlot(), isSameAsLeaderSlot);
                }
            }
        }

        /**
         * @return Any node that is a LEADER or INCUMBENT. Throws an exception if there is no such node. It is up to the caller to
         * check the modes of all the other nodes to make sure, if expected, that there are no other LEADER/INCUMBENT nodes.
         */
        private ClusterNode getAnyLeader() {
            return clusterNodes.stream().filter(clusterNode -> clusterNode.legislator.isLeading()).findFirst().get();
        }

        private void doNextWakeUp() {
            final long firstWakeUp = clusterNodes.stream()
                .map(clusterNode -> clusterNode.legislator.getNextWakeUpTimeMillis()).min(Long::compareTo).get();

            setCurrentTimeForwards(firstWakeUp);

            for (final ClusterNode clusterNode : clusterNodes) {
                if (clusterNode.legislator.getNextWakeUpTimeMillis() <= currentTimeMillis) {
                    logger.info("waking up {} at {}ms", clusterNode.getId(), currentTimeMillis);
                    try {
                        clusterNode.legislator.handleWakeUp();
                        break; // There's a possibility that there's >1 node with the same wake-up time. Wake them up in order.
                    } catch (IllegalArgumentException e) {
                        // This is ok: it just means a message couldn't currently be handled.
                    }
                }
            }
        }

        private void runRandomly(boolean reconfigure) {
            // Safety phase: behave quite randomly and verify that there is no divergence, but without any expectation of progress.
            logger.info("--> start of safety phase");

            for (int iteration = 0; iteration < 10000; iteration++) {
                try {
                    if (pendingActions.size() > 0 && usually()) {
                        deliverRandomMessage();
                    } else if (rarely()) {
                        // send a client value to a random node, preferring leaders
                        final ClusterNode clusterNode = randomLegislatorPreferringLeaders();
                        final int newValue = randomInt();
                        logger.info("----> [safety {}] proposing new value [{}] to [{}]", iteration, newValue, clusterNode.getId());
                        clusterNode.legislator.handleClientValue(ConsensusStateTests.createUpdate("set value to " + newValue,
                            cs -> new ClusterState(cs.getSlot() + 1, cs.getVotingNodes(), newValue)));
                    } else if (reconfigure && rarely()) {
                        // perform a reconfiguration
                        final ClusterNode clusterNode = randomLegislatorPreferringLeaders();
                        final NodeCollection newConfig = randomConfiguration();
                        logger.info("----> [safety {}] proposing reconfig [{}] to [{}]", iteration, newConfig, clusterNode.getId());
                        clusterNode.legislator.handleClientValue(ConsensusStateTests.createUpdate("update config to " + newConfig,
                            cs -> new ClusterState(cs.getSlot() + 1, newConfig, cs.getValue())));
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
                    } else {
                        // wake up random node
                        // TODO @ywelsch says "call this "long garbage collection cycle" ;-)" - discuss this
                        final ClusterNode clusterNode = randomLegislator();
                        logger.info("----> [safety {}] waking up [{}]", iteration, clusterNode.getId());
                        setCurrentTimeForwards(clusterNode.legislator.getNextWakeUpTimeMillis());
                        clusterNode.legislator.handleWakeUp();
                    }
                } catch (IllegalArgumentException e) {
                    // This is ok: it just means a message couldn't currently be handled.
                }

                assertConsistentStates();
            }

            logger.info("--> end of safety phase");
        }

        void setCurrentTimeForwards(long newTimeMillis) {
            if (newTimeMillis > currentTimeMillis) {
                currentTimeMillis = newTimeMillis;
            }
        }

        void sendTo(DiscoveryNode destination, Consumer<Legislator<ClusterState>> action) {
            pendingActions.add(() -> {
                for (final ClusterNode clusterNode : clusterNodes) {
                    if (clusterNode.localNode == destination && clusterNode.isConnected) {
                        action.accept(clusterNode.legislator);
                    }
                }
            });
        }

        void broadcast(Consumer<Legislator<ClusterState>> action) {
            for (final ClusterNode clusterNode : clusterNodes) {
                sendTo(clusterNode.localNode, action);
            }
        }

        void sendVoteFrom(DiscoveryNode sender, DiscoveryNode destination, Vote vote) {
            sendTo(destination, e -> e.handleVote(sender, vote));
        }

        void broadcastPublishRequestFrom(DiscoveryNode sender, PublishRequest<ClusterState> publishRequest) {
            broadcast(e -> e.handlePublishRequest(sender, publishRequest));
        }

        void sendPublishResponseFrom(DiscoveryNode sender, DiscoveryNode destination, PublishResponse publishResponse) {
            sendTo(destination, e -> e.handlePublishResponse(sender, publishResponse));
        }

        void broadcastApplyCommitFrom(DiscoveryNode thisNode, ApplyCommit applyCommit) {
            broadcast(e -> e.handleApplyCommit(thisNode, applyCommit));
        }

        void broadcastSeekVotesFrom(DiscoveryNode sender, SeekVotes seekVotes) {
            broadcast(e -> e.handleSeekVotes(sender, seekVotes));
        }

        void sendOfferVoteFrom(DiscoveryNode sender, DiscoveryNode destination, OfferVote offerVote) {
            sendTo(destination, e -> e.handleOfferVote(sender, offerVote));
        }

        void broadcastStartVoteFrom(DiscoveryNode sender, long term) {
            broadcast(e -> e.handleStartVote(sender, term));
        }

        void sendPreVoteHandoverFrom(DiscoveryNode sender, DiscoveryNode destination) {
            sendTo(destination, e -> e.handlePreVoteHandover(sender));
        }

        void sendRequestCatchUpFrom(DiscoveryNode sender, DiscoveryNode destination) {
            sendTo(destination, e -> e.handleRequestCatchUp(sender));
        }

        void sendCatchUpFrom(DiscoveryNode sender, DiscoveryNode destination, ClusterState catchUp) {
            sendTo(destination, e -> e.handleCatchUp(sender, catchUp));
        }

        void sendAbdicationFrom(DiscoveryNode sender, DiscoveryNode destination, long currentTerm) {
            sendTo(destination, e -> e.handleAbdication(sender, currentTerm));
        }

        private void assertConsistentStates() {
            Map<Long, ClusterState> statesBySlot = new HashMap<>();
            for (final ClusterNode clusterNode : clusterNodes) {
                ClusterState committedState = clusterNode.legislator.getCommittedState();
                ClusterState storedState = statesBySlot.get(committedState.getSlot());
                if (storedState == null) {
                    statesBySlot.put(committedState.getSlot(), committedState);
                } else {
                    assertEquals(committedState.getVotingNodes(), storedState.getVotingNodes());
                    assertEquals(committedState.getValue(), storedState.getValue());
                }
            }
        }

        private void deliverNextMessage() {
            // TODO count message delays and assert bounds on this number
            pendingActions.remove(0).run();
        }

        private void deliverNextMessageUntilQuiescent() {
            while (pendingActions.size() > 0) {
                try {
                    deliverNextMessage();
                } catch (IllegalArgumentException e) {
                    // This is ok: it just means a message couldn't currently be handled.
                }
                assertConsistentStates();
            }
        }

        private void deliverRandomMessage() {
            Runnable action = pendingActions.remove(randomInt(pendingActions.size() - 1));
            if (usually()) {
                action.run();
            }
        }

        private NodeCollection randomConfiguration() {
            final NodeCollection configuration = new NodeCollection();
            for (final ClusterNode clusterNode : randomSubsetOf(randomIntBetween(1, clusterNodes.size()), clusterNodes)) {
                configuration.add(clusterNode.localNode);
            }
            return configuration;
        }

        private ClusterNode randomLegislator() {
            return randomFrom(clusterNodes);
        }

        private ClusterNode randomLegislatorPreferringLeaders() {
            for (int i = 0; i < 3; i++) {
                ClusterNode clusterNode = randomLegislator();
                if (clusterNode.legislator.isLeading()) {
                    return clusterNode;
                }
            }
            return randomLegislator();
        }

        class ClusterNode {
            final DiscoveryNode localNode;
            final MockPersistence persistence;
            final MockTransport transport;
            Legislator<ClusterState> legislator;
            boolean isConnected = true;

            ClusterNode(int index) {
                localNode = new DiscoveryNode("node" + index, buildNewFakeTransportAddress(), Version.CURRENT);
                transport = new MockTransport();
                persistence = new MockPersistence();
            }

            void initialise(NodeCollection initialVotingNodes) {
                assert legislator == null;
                persistence.persistCommittedState(new ClusterState(-1, initialVotingNodes, 0));
                reboot();
            }

            void reboot() {
                legislator = persistence.load();
            }

            String getId() {
                return localNode.getId();
            }

            private class MockTransport implements Transport<ClusterState> {
                @Override
                public void sendVote(DiscoveryNode destination, Vote vote) {
                    if (isConnected) {
                        sendVoteFrom(localNode, destination, vote);
                    }
                }

                @Override
                public void broadcastPublishRequest(PublishRequest<ClusterState> publishRequest) {
                    if (isConnected) {
                        broadcastPublishRequestFrom(localNode, publishRequest);
                    }
                }

                @Override
                public void sendPublishResponse(DiscoveryNode destination, PublishResponse publishResponse) {
                    if (isConnected) {
                        sendPublishResponseFrom(localNode, destination, publishResponse);
                    }
                }

                @Override
                public void broadcastApplyCommit(ApplyCommit applyCommit) {
                    if (isConnected) {
                        broadcastApplyCommitFrom(localNode, applyCommit);
                    }
                }

                @Override
                public void broadcastSeekVotes(SeekVotes seekVotes) {
                    if (isConnected) {
                        broadcastSeekVotesFrom(localNode, seekVotes);
                    }
                }

                @Override
                public void sendOfferVote(DiscoveryNode destination, OfferVote offerVote) {
                    if (isConnected) {
                        sendOfferVoteFrom(localNode, destination, offerVote);
                    }
                }

                @Override
                public void broadcastStartVote(long term) {
                    if (isConnected) {
                        broadcastStartVoteFrom(localNode, term);
                    }
                }

                @Override
                public void sendPreVoteHandover(DiscoveryNode destination) {
                    if (isConnected) {
                        sendPreVoteHandoverFrom(localNode, destination);
                    }
                }

                @Override
                public void sendRequestCatchUp(DiscoveryNode destination) {
                    if (isConnected) {
                        sendRequestCatchUpFrom(localNode, destination);
                    }
                }

                @Override
                public void sendCatchUp(DiscoveryNode destination, ClusterState catchUp) {
                    if (isConnected) {
                        sendCatchUpFrom(localNode, destination, catchUp);
                    }
                }

                @Override
                public void sendAbdication(DiscoveryNode destination, long currentTerm) {
                    if (isConnected) {
                        sendAbdicationFrom(localNode, destination, currentTerm);
                    }
                }
            }

            // TODO 1. use `BasePersistedState` once the persistence layer is merged
            // TODO 2. also have some tests that use the on-disk data for persistence across reboots.
            private class MockPersistence implements Persistence<ClusterState> {

                private long currentTerm;
                private ClusterState committedState;
                private Optional<AcceptedState<ClusterState>> acceptedState;

                MockPersistence() {
                    currentTerm = 0;
                    acceptedState = Optional.empty();
                    // committedState needs to be initialised later as it needs to know the initial configuration
                }

                @Override
                public void persistCurrentTerm(long currentTerm) {
                    this.currentTerm = currentTerm;
                }

                @Override
                public void persistCommittedState(ClusterState committedState) {
                    this.committedState = committedState;
                    this.acceptedState = Optional.empty();
                }

                @Override
                public void persistAcceptedState(AcceptedState<ClusterState> acceptedState) {
                    this.acceptedState = Optional.of(acceptedState);
                }

                public Legislator<ClusterState> load() {
                    Settings.Builder sb = Settings.builder();
                    sb.put("node.name", localNode.getId());
                    return new Legislator<>(sb.build(), currentTerm, committedState, acceptedState, this, transport, localNode,
                        new CurrentTimeSupplier(),
                        ConsensusStateTests.createUpdate("no-op",
                            cs -> new ClusterState(cs.getSlot() + 1, cs.getVotingNodes(), cs.getValue())));
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
