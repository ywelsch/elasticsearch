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
import org.elasticsearch.discovery.zen2.ConsensusState.BasePersistedState;
import org.elasticsearch.discovery.zen2.ConsensusState.NodeCollection;
import org.elasticsearch.discovery.zen2.ConsensusState.PersistedState;
import org.elasticsearch.discovery.zen2.ConsensusStateTests.ClusterState;
import org.elasticsearch.discovery.zen2.Legislator.Transport;
import org.elasticsearch.discovery.zen2.LegislatorTests.Cluster.ClusterNode;
import org.elasticsearch.discovery.zen2.Messages.ApplyCommit;
import org.elasticsearch.discovery.zen2.Messages.CatchupRequest;
import org.elasticsearch.discovery.zen2.Messages.HeartbeatRequest;
import org.elasticsearch.discovery.zen2.Messages.HeartbeatResponse;
import org.elasticsearch.discovery.zen2.Messages.LegislatorPublishResponse;
import org.elasticsearch.discovery.zen2.Messages.OfferVote;
import org.elasticsearch.discovery.zen2.Messages.PublishRequest;
import org.elasticsearch.discovery.zen2.Messages.PublishResponse;
import org.elasticsearch.discovery.zen2.Messages.SeekVotes;
import org.elasticsearch.discovery.zen2.Messages.StartVoteRequest;
import org.elasticsearch.discovery.zen2.Messages.Vote;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import static org.elasticsearch.discovery.zen2.ConsensusStateTests.diffWithValue;
import static org.elasticsearch.discovery.zen2.ConsensusStateTests.diffWithVotingNodes;
import static org.elasticsearch.discovery.zen2.Legislator.CONSENSUS_FOLLOWER_TIMEOUT_SETTING;
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
        leader.legislator.handleClientValue(diffWithValue(leader.legislator.getCommittedState(), finalValue));
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

        leader.legislator.handleClientValue(diffWithVotingNodes(leader.legislator.getCommittedState(), allNodes));
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

        logger.info("--> start of reconfiguration to make all nodes into voting nodes");

        leader.legislator.handleClientValue(diffWithVotingNodes(leader.legislator.getCommittedState(), allNodes));
        cluster.deliverNextMessageUntilQuiescent();

        logger.info("--> end of reconfiguration to make all nodes into voting nodes");

        final long disconnectionTime = cluster.currentTimeMillis;
        leader.isConnected = false;
        leader.legislator.ignoreWakeUpsForAtLeast(TimeValue.timeValueSeconds(10));

        for (ClusterNode clusterNode : cluster.clusterNodes) {
            if (clusterNode != leader) {
                logger.info("--> notifying {} of leader disconnection", clusterNode.getLocalNode());
                clusterNode.legislator.handleDisconnectedNode(leader.localNode);
            }
        }


        // The nodes all entered mode CANDIDATE, so the next wake-up should be after a delay of at most 2 * CONSENSUS_MIN_DELAY_SETTING.
        final long minDelayMillis = CONSENSUS_MIN_DELAY_SETTING.get(Settings.EMPTY).millis();
        final long firstWakeUpTime = cluster.clusterNodes.stream()
            .map(ClusterNode::getLegislator).map(Legislator::getNextWakeUpTimeMillis).min(Long::compare).get();
        assertThat("re-election time is short", firstWakeUpTime - disconnectionTime, lessThan(minDelayMillis * 2));

        // Run until the first wake-up time, accounting for variability in the scheduler.
        final long stabilisationTime = disconnectionTime + minDelayMillis * 2 + Cluster.DEFAULT_DELAY_VARIABILITY;
        logger.info("--> performing wake-ups until [{}ms]", stabilisationTime);
        while (cluster.getNextTaskExecutionTime() < stabilisationTime) {
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
        private final List<Runnable> pendingActions = new ArrayList<>();
        private long currentTimeMillis = 0L;
        private static final long DEFAULT_DELAY_VARIABILITY = 100L;
        private static final long RANDOM_MODE_DELAY_VARIABILITY = 10000L;

        Cluster(int nodeCount) {
            clusterNodes = new ArrayList<>(nodeCount);
            logger.info("--> creating cluster of {} nodes", nodeCount);

            for (int i = 0; i < nodeCount; i++) {
                clusterNodes.add(new ClusterNode(i));
            }

            final NodeCollection initialConfiguration = randomConfiguration();
            logger.info("--> initial configuration: {}", initialConfiguration);
            for (final ClusterNode clusterNode : clusterNodes) {
                clusterNode.initialise(initialConfiguration);
            }
        }

        private void stabilise() {
            // Stabilisation phase: just wake up nodes in order for long enough to allow a leader to be elected
            deliverNextMessageUntilQuiescent();

            // How long to wait? The worst case is that a leader just committed a value to all the other nodes, and then
            // dropped off the network, which would mean that they all wait for up to CONSENSUS_FOLLOWER_TIMEOUT_SETTING
            // before waking up again. Then they wake up, become candidates, and wait for up to
            // 2 * CONSENSUS_MIN_DELAY_SETTING before attempting an election. The first election is expected to succeed, however,
            // because we run to quiescence before waking any other nodes up.
            final long catchUpPhaseEndMillis = currentTimeMillis +
                CONSENSUS_FOLLOWER_TIMEOUT_SETTING.get(Settings.EMPTY).millis() +
                CONSENSUS_MIN_DELAY_SETTING.get(Settings.EMPTY).millis() * 2 +
                RANDOM_MODE_DELAY_VARIABILITY + DEFAULT_DELAY_VARIABILITY;

            logger.info("--> start of stabilisation phase: run until time {}ms", catchUpPhaseEndMillis);
            setDelayVariability(DEFAULT_DELAY_VARIABILITY);

            while (currentTimeMillis < catchUpPhaseEndMillis) {
                doNextWakeUp();
                deliverNextMessageUntilQuiescent();
            }

            logger.info("--> end of stabilisation phase");

            assertUniqueLeaderAndExpectedModes();
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
         * @return Any node that is a LEADER. Throws an exception if there is no such node. It is up to the caller to
         * check the modes of all the other nodes to make sure, if expected, that there are no other LEADER nodes.
         */
        private ClusterNode getAnyLeader() {
            List<ClusterNode> leaders = clusterNodes.stream()
                .filter(clusterNode -> clusterNode.legislator.getMode() == Legislator.Mode.LEADER)
                .collect(Collectors.toList());
            assertNotEquals(Collections.emptyList(), leaders);
            return randomFrom(leaders);
        }

        private final List<ClusterNode.TaskWithExecutionTime> tasks = new ArrayList<>();

        private void runRandomly(boolean reconfigure) {
            // Safety phase: behave quite randomly and verify that there is no divergence, but without any expectation of progress.
            logger.info("--> start of safety phase");
            setDelayVariability(RANDOM_MODE_DELAY_VARIABILITY);

            for (int iteration = 0; iteration < 10000; iteration++) {
                try {
                    if (pendingActions.size() > 0 && usually()) {
                        deliverRandomMessage();
                    } else if (rarely()) {
                        // send a client value to a random node, preferring leaders
                        final ClusterNode clusterNode = randomLegislatorPreferringLeaders();
                        final int newValue = randomInt();
                        logger.info("----> [safety {}] proposing new value [{}] to [{}]", iteration, newValue, clusterNode.getId());
                        clusterNode.legislator.handleClientValue(diffWithValue(clusterNode.legislator.getCommittedState(), newValue));
                    } else if (reconfigure && rarely()) {
                        // perform a reconfiguration
                        final ClusterNode clusterNode = randomLegislatorPreferringLeaders();
                        final NodeCollection newConfig = randomConfiguration();
                        logger.info("----> [safety {}] proposing reconfig [{}] to [{}]", iteration, newConfig, clusterNode.getId());
                        clusterNode.legislator.handleClientValue(
                            diffWithVotingNodes(clusterNode.legislator.getCommittedState(), newConfig));
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
                    } else {
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
                    task.task.run();
                    break; // in case there is more than one task with the same execution time
                }
            }
        }

        private Long getNextTaskExecutionTime() {
            return tasks.stream()
                .map(ClusterNode.TaskWithExecutionTime::getExecutionTimeMillis).min(Long::compareTo).get();
        }

        private long delayVariability = DEFAULT_DELAY_VARIABILITY;

        void sendTo(DiscoveryNode destination, Consumer<Legislator<ClusterState>> action) {
            pendingActions.add(() -> {
                for (final ClusterNode clusterNode : clusterNodes) {
                    if (clusterNode.localNode == destination && clusterNode.isConnected) {
                        action.accept(clusterNode.legislator);
                    }
                }
            });
        }

        void sendPublishRequestFrom(DiscoveryNode sender, DiscoveryNode destination, PublishRequest<ClusterState> publishRequest,
                                    TransportResponseHandler<LegislatorPublishResponse> responseHandler) {
            sendTo(destination, e -> {
                try {
                    LegislatorPublishResponse publishResponse = e.handlePublishRequest(sender, publishRequest);
                    sendTo(sender, e2 -> responseHandler.handleResponse(publishResponse));
                } catch (Exception ex) {
                    sendTo(sender, e2 -> responseHandler.handleException(new TransportException(ex)));
                }
            });
        }

        void sendHeartbeatRequestFrom(DiscoveryNode sender, DiscoveryNode destination, HeartbeatRequest heartbeatRequest,
                                      TransportResponseHandler<HeartbeatResponse> responseHandler) {
            sendTo(destination, e -> {
                try {
                    HeartbeatResponse heartbeatResponse = e.handleHeartbeatRequest(sender, heartbeatRequest);
                    sendTo(sender, e2 -> responseHandler.handleResponse(heartbeatResponse));
                } catch (Exception ex) {
                    sendTo(sender, e2 -> responseHandler.handleException(new TransportException(ex)));
                }
            });
        }

        void sendApplyCommitFrom(DiscoveryNode sender, DiscoveryNode destination, ApplyCommit applyCommit,
                                 TransportResponseHandler<TransportResponse.Empty> responseHandler) {
            sendTo(destination, e -> {
                try {
                    e.handleApplyCommit(sender, applyCommit);
                    sendTo(sender, e2 -> responseHandler.handleResponse(TransportResponse.Empty.INSTANCE));
                } catch (Exception ex) {
                    sendTo(sender, e2 -> responseHandler.handleException(new TransportException(ex)));
                }
            });
        }

        void sendSeekVotesFrom(DiscoveryNode sender, DiscoveryNode destination, SeekVotes seekVotes,
                               TransportResponseHandler<OfferVote> responseHandler) {
            sendTo(destination, e -> {
                try {
                    OfferVote offerVote = e.handleSeekVotes(sender, seekVotes);
                    sendTo(sender, e2 -> responseHandler.handleResponse(offerVote));
                } catch (Exception ex) {
                    sendTo(sender, e2 -> responseHandler.handleException(new TransportException(ex)));
                }
            });
        }

        void sendStartVoteFrom(DiscoveryNode sender, DiscoveryNode destination, StartVoteRequest startVoteRequest,
                               TransportResponseHandler<Vote> responseHandler) {
            sendTo(destination, e -> {
                try {
                    Vote vote = e.handleStartVote(sender, startVoteRequest);
                    sendTo(sender, e2 -> responseHandler.handleResponse(vote));
                } catch (Exception ex) {
                    sendTo(sender, e2 -> responseHandler.handleException(new TransportException(ex)));
                }
            });
        }

        void sendPreVoteHandoverFrom(DiscoveryNode sender, DiscoveryNode destination) {
            sendTo(destination, e -> e.handlePreVoteHandover(sender));
        }

        void sendCatchUpFrom(DiscoveryNode sender, DiscoveryNode destination, CatchupRequest<ClusterState> catchUp,
                             TransportResponseHandler<PublishResponse> responseHandler) {
            sendTo(destination, e -> {
                try {
                    PublishResponse publishResponse = e.handleCatchUp(sender, catchUp);
                    sendTo(sender, e2 -> responseHandler.handleResponse(publishResponse));
                } catch (Exception ex) {
                    sendTo(sender, e2 -> responseHandler.handleException(new TransportException(ex)));
                }
            });
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
                clusterNode.legislator.invariant();
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
                } catch (ConsensusMessageRejectedException ignored) {
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
                if (clusterNode.legislator.getMode() == Legislator.Mode.LEADER) {
                    return clusterNode;
                }
            }
            return randomLegislator();
        }

        public void setDelayVariability(long delayVariability) {
            this.delayVariability = delayVariability;
        }

        class ClusterNode {
            final DiscoveryNode localNode;
            PersistedState<ClusterState> persistedState;
            final MockTransport transport;
            Legislator<ClusterState> legislator;
            boolean isConnected = true;

            ClusterNode(int index) {
                localNode = new DiscoveryNode("node" + index, buildNewFakeTransportAddress(), Version.CURRENT);
                transport = new MockTransport();
            }

            void initialise(NodeCollection initialVotingNodes) {
                assert persistedState == null;
                assert legislator == null;
                persistedState = new BasePersistedState<>(0L, new ClusterState(-1, initialVotingNodes, 0));
                legislator = createLegislator();
            }

            // TODO: have some tests that use the on-disk data for persistence across reboots.
            void reboot() {
                tasks.removeIf(task -> task.getLegislator() == legislator);
                legislator = createLegislator();
            }

            private Legislator<ClusterState> createLegislator() {
                Settings settings = Settings.builder()
                    .put("node.name", localNode.getId())
                    .build();
                return new Legislator<>(settings, persistedState, transport, localNode,
                    new CurrentTimeSupplier(), new FutureExecutor(),
                    () -> clusterNodes.stream().map(ClusterNode::getLocalNode).collect(Collectors.toList()),
                    ConsensusStateTests::noOpDiff);
            }

            String getId() {
                return localNode.getId();
            }

            public DiscoveryNode getLocalNode() {
                return localNode;
            }

            public Legislator<ClusterState> getLegislator() {
                return legislator;
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
                    tasks.add(new TaskWithExecutionTime(executionTimeMillis, task));
                }
            }

            private class MockTransport implements Transport<ClusterState> {

                @Override
                public void sendPublishRequest(DiscoveryNode destination, PublishRequest<ClusterState> publishRequest,
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
                public void sendSeekVotes(DiscoveryNode destination, SeekVotes seekVotes,
                                          TransportResponseHandler<OfferVote> responseHandler) {
                    if (isConnected) {
                        sendSeekVotesFrom(localNode, destination, seekVotes, responseHandler);
                    }
                }

                @Override
                public void sendStartVote(DiscoveryNode destination, StartVoteRequest startVoteRequest,
                                          TransportResponseHandler<Vote> responseHandler) {
                    if (isConnected) {
                        sendStartVoteFrom(localNode, destination, startVoteRequest, responseHandler);
                    }
                }

                @Override
                public void sendPreVoteHandover(DiscoveryNode destination) {
                    if (isConnected) {
                        sendPreVoteHandoverFrom(localNode, destination);
                    }
                }

                @Override
                public void sendCatchUp(DiscoveryNode destination, CatchupRequest<ClusterState> catchUp,
                                        TransportResponseHandler<PublishResponse> responseHandler) {
                    if (isConnected) {
                        sendCatchUpFrom(localNode, destination, catchUp, responseHandler);
                    }
                }

                @Override
                public void sendAbdication(DiscoveryNode destination, long currentTerm) {
                    if (isConnected) {
                        sendAbdicationFrom(localNode, destination, currentTerm);
                    }
                }
            }

            private class TaskWithExecutionTime {
                final long executionTimeMillis;
                final Runnable task;

                TaskWithExecutionTime(long executionTimeMillis, Runnable task) {
                    this.executionTimeMillis = executionTimeMillis;
                    this.task = task;
                }

                public long getExecutionTimeMillis() {
                    return executionTimeMillis;
                }

                @Override
                public String toString() {
                    return "task on [" + legislator.getLocalNode() + "] scheduled at time [" + executionTimeMillis + "ms]";
                }

                public Legislator<ClusterState> getLegislator() {
                    return legislator;
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
