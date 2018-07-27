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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterState.VotingConfiguration;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.zen.PublishClusterStateActionTests;
import org.elasticsearch.discovery.zen.PublishClusterStateActionTests.AssertingAckListener;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponse;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class PublicationTests extends ESTestCase {

    class MockNode {

        MockNode(Settings settings, DiscoveryNode localNode) {
            this.localNode = localNode;
            ClusterState initialState = CoordinationStateTests.clusterState(0L, 0L, localNode,
                VotingConfiguration.EMPTY_CONFIG, VotingConfiguration.EMPTY_CONFIG, 0L);
            coordinationState = new CoordinationState(settings, localNode, new CoordinationStateTests.InMemoryPersistedState(0L,
                initialState));
        }

        final DiscoveryNode localNode;

        final CoordinationState coordinationState;

        public MockPublication publish(ClusterState clusterState, Discovery.AckListener ackListener, Set<DiscoveryNode> faultyNodes) {
            PublishRequest publishRequest = coordinationState.handleClientValue(clusterState);
            MockPublication currentPublication = new MockPublication(Settings.EMPTY, publishRequest, ackListener, () -> 0L) {
                @Override
                protected boolean isPublishQuorum(CoordinationState.VoteCollection votes) {
                    return coordinationState.isPublishQuorum(votes);
                }

                @Override
                protected Optional<ApplyCommitRequest> handlePublishResponse(DiscoveryNode sourceNode, PublishResponse publishResponse) {
                    return coordinationState.handlePublishResponse(sourceNode, publishResponse);
                }
            };
            currentPublication.start(faultyNodes);
            return currentPublication;
        }
    }

    abstract class MockPublication extends Publication {

        final PublishRequest publishRequest;

        ApplyCommitRequest applyCommit;

        boolean completed;

        boolean success;

        Map<DiscoveryNode, ActionListener<PublishWithJoinResponse>> pendingPublications = new HashMap<>();
        Map<DiscoveryNode, ActionListener<TransportResponse.Empty>> pendingCommits = new HashMap<>();

        public MockPublication(Settings settings, PublishRequest publishRequest, Discovery.AckListener ackListener,
                               LongSupplier currentTimeSupplier) {
            super(settings, publishRequest, ackListener, currentTimeSupplier);
            this.publishRequest = publishRequest;
        }

        @Override
        protected void onCompletion(boolean success) {
            completed = true;
            this.success = success;
        }

        @Override
        protected void onPossibleJoin(DiscoveryNode sourceNode, PublishWithJoinResponse response) {

        }

        @Override
        protected void sendPublishRequest(DiscoveryNode destination, PublishRequest publishRequest,
                                          ActionListener<PublishWithJoinResponse> responseActionListener) {
            assertSame(publishRequest, this.publishRequest);
            assertNull(pendingPublications.put(destination, responseActionListener));
        }

        @Override
        protected void sendApplyCommit(DiscoveryNode destination, ApplyCommitRequest applyCommit,
                                       ActionListener<TransportResponse.Empty> responseActionListener) {
            if (this.applyCommit == null) {
                this.applyCommit = applyCommit;
            } else {
                assertSame(applyCommit, this.applyCommit);
            }
            assertNull(pendingCommits.put(destination, responseActionListener));
        }
    }

    DiscoveryNode n1 = CoordinationStateTests.createNode("node1");
    DiscoveryNode n2 = CoordinationStateTests.createNode("node2");
    DiscoveryNode n3 = CoordinationStateTests.createNode("node3");
    Set<DiscoveryNode> discoNodes = Sets.newHashSet(n1, n2, n3);

    MockNode node1 = new MockNode(Settings.EMPTY, n1);
    MockNode node2 = new MockNode(Settings.EMPTY, n2);
    MockNode node3 = new MockNode(Settings.EMPTY, n3);
    List<MockNode> nodes = Arrays.asList(node1, node2, node3);

    Function<DiscoveryNode, MockNode> nodeResolver = dn -> nodes.stream().filter(mn -> mn.localNode.equals(dn)).findFirst().get();

    private void initializeCluster(VotingConfiguration initialConfig) {
        node1.coordinationState.setInitialState(CoordinationStateTests.clusterState(0L, 1L, n1, initialConfig, initialConfig, 0L));
        StartJoinRequest startJoinRequest = new StartJoinRequest(n1, 1L);
        node1.coordinationState.handleJoin(node1.coordinationState.handleStartJoin(startJoinRequest));
        node1.coordinationState.handleJoin(node2.coordinationState.handleStartJoin(startJoinRequest));
        node1.coordinationState.handleJoin(node3.coordinationState.handleStartJoin(startJoinRequest));
        assertTrue(node1.coordinationState.electionWon());
    }

    public void testSimpleClusterStatePublishing() throws InterruptedException {
        VotingConfiguration singleNodeConfig = new VotingConfiguration(Sets.newHashSet(n1.getId()));
        initializeCluster(singleNodeConfig);

        AssertingAckListener ackListener = new AssertingAckListener(nodes.size());
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().add(n1).add(n2).add(n3).localNodeId(n1.getId()).build();
        MockPublication publication = node1.publish(CoordinationStateTests.clusterState(1L, 2L,
            discoveryNodes, singleNodeConfig, singleNodeConfig, 42L), ackListener, Collections.emptySet());

        assertThat(publication.pendingPublications.keySet(), equalTo(discoNodes));
        assertTrue(publication.pendingCommits.isEmpty());
        publication.pendingPublications.entrySet().stream().collect(shuffle()).forEach(e -> {
            PublishResponse publishResponse = nodeResolver.apply(e.getKey()).coordinationState.handlePublishRequest(
                publication.publishRequest);
            e.getValue().onResponse(new PublishWithJoinResponse(publishResponse, Optional.empty()));
        });

        assertThat(publication.pendingCommits.keySet(), equalTo(discoNodes));
        assertNotNull(publication.applyCommit);
        assertEquals(publication.applyCommit.getTerm(), publication.publishRequest.getAcceptedState().term());
        assertEquals(publication.applyCommit.getVersion(), publication.publishRequest.getAcceptedState().version());
        publication.pendingCommits.entrySet().stream().collect(shuffle()).forEach(e -> {
            nodeResolver.apply(e.getKey()).coordinationState.handleCommit(publication.applyCommit);
            e.getValue().onResponse(TransportResponse.Empty.INSTANCE);
        });

        assertTrue(publication.completed);
        assertTrue(publication.success);

        ackListener.await(0L, TimeUnit.SECONDS);
    }

    public void testClusterStatePublishingWithFaultyNode() throws InterruptedException {
        VotingConfiguration singleNodeConfig = new VotingConfiguration(Sets.newHashSet(n1.getId()));
        initializeCluster(singleNodeConfig);

        AssertingAckListener ackListener = new AssertingAckListener(nodes.size());
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().add(n1).add(n2).add(n3).localNodeId(n1.getId()).build();

        boolean failNodeWhenCommitting = randomBoolean();
        boolean publicationDidNotMakeItToNode2 = randomBoolean();
        AtomicInteger remainingActions = new AtomicInteger(
            failNodeWhenCommitting ? (publicationDidNotMakeItToNode2 ? 2 : 3) : 4);
        int injectFaultAt = randomInt(remainingActions.get() - 1);
        logger.info("Injecting fault at: {}, failNodeWhenCommitting: {}, publicationDidNotMakeItToNode2: {}", injectFaultAt,
            failNodeWhenCommitting, publicationDidNotMakeItToNode2);

        Set<DiscoveryNode> initialFaultyNodes =
            failNodeWhenCommitting == false && remainingActions.decrementAndGet() == injectFaultAt ?
                Collections.singleton(n2) : Collections.emptySet();
        MockPublication publication = node1.publish(CoordinationStateTests.clusterState(1L, 2L,
            discoveryNodes, singleNodeConfig, singleNodeConfig, 42L), ackListener, initialFaultyNodes);

        publication.pendingPublications.entrySet().stream().collect(shuffle()).forEach(e -> {
            if (failNodeWhenCommitting == false) {
                if (remainingActions.decrementAndGet() == injectFaultAt) {
                    publication.onFaultyNode(n2);
                }
                if (e.getKey().equals(n2) == false || randomBoolean()) {
                    PublishResponse publishResponse = nodeResolver.apply(e.getKey()).coordinationState.handlePublishRequest(
                        publication.publishRequest);
                    e.getValue().onResponse(new PublishWithJoinResponse(publishResponse, Optional.empty()));
                }
            } else {
                if (e.getKey().equals(n2) == false || publicationDidNotMakeItToNode2 == false) {
                    PublishResponse publishResponse = nodeResolver.apply(e.getKey()).coordinationState.handlePublishRequest(
                        publication.publishRequest);
                    e.getValue().onResponse(new PublishWithJoinResponse(publishResponse, Optional.empty()));
                }
            }
        });

        publication.pendingCommits.entrySet().stream().collect(shuffle()).forEach(e -> {
            if (failNodeWhenCommitting == false) {
                nodeResolver.apply(e.getKey()).coordinationState.handleCommit(publication.applyCommit);
                e.getValue().onResponse(TransportResponse.Empty.INSTANCE);
            } else {
                if (e.getKey().equals(n2)) {
                    // we must fail node before committing for the node, otherwise failing the node is ignored
                    publication.onFaultyNode(n2);
                }
                if (remainingActions.decrementAndGet() == injectFaultAt) {
                    publication.onFaultyNode(n2);
                }
                if (e.getKey().equals(n2) == false || randomBoolean()) {
                    nodeResolver.apply(e.getKey()).coordinationState.handleCommit(publication.applyCommit);
                    e.getValue().onResponse(TransportResponse.Empty.INSTANCE);
                }
            }
        });

        // we need to complete publication by failing the node
        if (failNodeWhenCommitting && publicationDidNotMakeItToNode2 && remainingActions.get() > injectFaultAt) {
            publication.onFaultyNode(n2);
        }

        assertTrue(publication.completed);
        assertTrue(publication.success);

        publication.onFaultyNode(randomFrom(n1, n3)); // has no influence

        List<Tuple<DiscoveryNode, Throwable>> errors = ackListener.awaitErrors(0L, TimeUnit.SECONDS);
        assertThat(errors.size(), equalTo(1));
        assertThat(errors.get(0).v1(), equalTo(n2));
    }

    public void testClusterStatePublishingFailsOrTimesOutBeforeCommit() throws InterruptedException {
        VotingConfiguration config = new VotingConfiguration(Sets.newHashSet(n1.getId(), n2.getId()));
        initializeCluster(config);

        AssertingAckListener ackListener = new AssertingAckListener(nodes.size());
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().add(n1).add(n2).add(n3).localNodeId(n1.getId()).build();
        MockPublication publication = node1.publish(CoordinationStateTests.clusterState(1L, 2L,
            discoveryNodes, config, config, 42L), ackListener, Collections.emptySet());

        publication.pendingPublications.entrySet().stream().collect(shuffle()).forEach(e -> {
            if (e.getKey().equals(n2)) {
                if (randomBoolean()) {
                    publication.onTimeout();
                } else {
                    e.getValue().onFailure(new TransportException(new Exception("dummy failure")));
                }
                assertTrue(publication.completed);
                assertFalse(publication.success);
            } else if (randomBoolean()) {
                PublishResponse publishResponse = nodeResolver.apply(e.getKey()).coordinationState.handlePublishRequest(
                    publication.publishRequest);
                e.getValue().onResponse(new PublishWithJoinResponse(publishResponse, Optional.empty()));
            }
        });

        assertThat(publication.pendingCommits.keySet(), equalTo(Collections.emptySet()));
        assertNull(publication.applyCommit);
        assertTrue(publication.completed);
        assertFalse(publication.success);

        List<Tuple<DiscoveryNode, Throwable>> errors = ackListener.awaitErrors(0L, TimeUnit.SECONDS);
        assertThat(errors.size(), equalTo(2)); // publication does not ack for the local node
    }

    public void testClusterStatePublishingTimesOutAfterCommit() throws InterruptedException {
        VotingConfiguration config = new VotingConfiguration(Sets.newHashSet(n1.getId(), n2.getId()));
        initializeCluster(config);

        AssertingAckListener ackListener = new AssertingAckListener(nodes.size());
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().add(n1).add(n2).add(n3).localNodeId(n1.getId()).build();
        MockPublication publication = node1.publish(CoordinationStateTests.clusterState(1L, 2L,
            discoveryNodes, config, config, 42L), ackListener, Collections.emptySet());

        boolean publishedToN3 = randomBoolean();
        publication.pendingPublications.entrySet().stream().collect(shuffle()).forEach(e -> {
            if (e.getKey().equals(n3) == false || publishedToN3) {
                PublishResponse publishResponse = nodeResolver.apply(e.getKey()).coordinationState.handlePublishRequest(
                    publication.publishRequest);
                e.getValue().onResponse(new PublishWithJoinResponse(publishResponse, Optional.empty()));
            }
        });

        assertNotNull(publication.applyCommit);

        Set<DiscoveryNode> committingNodes = new HashSet<>(randomSubsetOf(discoNodes));
        if (publishedToN3 == false) {
            committingNodes.remove(n3);
        }

        logger.info("Committing nodes: {}", committingNodes);

        publication.pendingCommits.entrySet().stream().collect(shuffle()).forEach(e -> {
            if (committingNodes.contains(e.getKey())) {
                nodeResolver.apply(e.getKey()).coordinationState.handleCommit(publication.applyCommit);
                e.getValue().onResponse(TransportResponse.Empty.INSTANCE);
            }
        });

        publication.onTimeout();
        assertTrue(publication.completed);

        if (committingNodes.contains(n1)) { // master needs to commit for publication to be successful
            assertTrue(publication.success);
        } else {
            assertFalse(publication.success);
        }

        Set<DiscoveryNode> ackedNodes = new HashSet<>(committingNodes);
        ackedNodes.remove(n1); // publication does not ack for the master node
        assertEquals(ackedNodes, ackListener.await(0L, TimeUnit.SECONDS));

        // check that acking still works after publication completed
        if (publishedToN3 == false) {
            publication.pendingPublications.get(n3).onResponse(
                new PublishWithJoinResponse(node3.coordinationState.handlePublishRequest(publication.publishRequest), Optional.empty()));
        }

        assertEquals(discoNodes, publication.pendingCommits.keySet());

        Set<DiscoveryNode> nonCommittedNodes = Sets.difference(discoNodes, committingNodes);
        logger.info("Non-committed nodes: {}", nonCommittedNodes);
        nonCommittedNodes.stream().collect(shuffle()).forEach(n -> {
                if (n.equals(n1) == false || randomBoolean()) {
                    publication.pendingCommits.get(n).onResponse(TransportResponse.Empty.INSTANCE);
                }
            });

        assertEquals(Sets.newHashSet(n2, n3), ackListener.await(0L, TimeUnit.SECONDS)); // publication does not ack for the local node
    }

    public static <T> Collector<T, ?, Stream<T>> shuffle() {
        return Collectors.collectingAndThen(Collectors.toList(),
            ts -> {
                Collections.shuffle(ts, random());
                return ts.stream();
            });
    }


}
