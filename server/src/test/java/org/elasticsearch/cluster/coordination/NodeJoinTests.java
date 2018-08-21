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

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterState.VotingConfiguration;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.cluster.service.MasterServiceTests;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.BaseFuture;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.indices.cluster.FakeThreadPoolMasterService;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@TestLogging("org.elasticsearch.cluster.service:TRACE,org.elasticsearch.cluster.coordination:TRACE")
public class NodeJoinTests extends ESTestCase {

    private static ThreadPool threadPool;

    private MasterService masterService;
    private Coordinator coordinator;
    private DeterministicTaskQueue deterministicTaskQueue;

    @BeforeClass
    public static void beforeClass() {
        threadPool = new TestThreadPool(NodeJoinTests.getTestClass().getName());
    }

    @AfterClass
    public static void afterClass() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        masterService.close();
    }

    private static ClusterState initialState(boolean withMaster, DiscoveryNode localNode, long term, long version,
                                             VotingConfiguration config) {
        ClusterState initialClusterState = ClusterState.builder(new ClusterName(ClusterServiceUtils.class.getSimpleName()))
            .nodes(DiscoveryNodes.builder()
                .add(localNode)
                .localNodeId(localNode.getId())
                .masterNodeId(withMaster ? localNode.getId() : null))
            .term(term)
            .version(version)
            .lastAcceptedConfiguration(config)
            .lastCommittedConfiguration(config)
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK).build();
        return initialClusterState;
    }

    private void setupFakeMasterServiceAndCoordinator(long term, ClusterState initialState) {
        deterministicTaskQueue = new DeterministicTaskQueue(Settings.EMPTY);
        FakeThreadPoolMasterService fakeMasterService = new FakeThreadPoolMasterService("test", deterministicTaskQueue::scheduleNow);
        AtomicReference<ClusterState> currentState = new AtomicReference<>(initialState);
        fakeMasterService.setClusterStateSupplier(currentState::get);
        fakeMasterService.setClusterStatePublisher((event, publishListener, ackListener) -> {
            currentState.set(event.state());
            publishListener.onResponse(null);
        });
        fakeMasterService.start();
        setupMasterServiceAndCoordinator(term, initialState, fakeMasterService);
    }

    private void setupRealMasterServiceAndCoordinator(long term, ClusterState initialState) {
        setupMasterServiceAndCoordinator(term, initialState, ClusterServiceUtils.createMasterService(threadPool, initialState));
    }

    private void setupMasterServiceAndCoordinator(long term, ClusterState initialState, MasterService masterService) {
        if (this.masterService != null || coordinator != null) {
            throw new IllegalStateException("method setupMasterServiceAndCoordinator can only be called once");
        }
        this.masterService = masterService;
        TransportService transportService = mock(TransportService.class);
        when(transportService.getLocalNode()).thenReturn(initialState.nodes().getLocalNode());
        coordinator = new Coordinator(Settings.EMPTY,
            transportService,
            ESAllocationTestCase.createAllocationService(Settings.EMPTY),
            masterService,
            () -> new CoordinationStateTests.InMemoryPersistedState(term, initialState));
        coordinator.start();
        coordinator.startInitialJoin();
    }

    protected DiscoveryNode newNode(int i) {
        return newNode(i, randomBoolean());
    }

    protected DiscoveryNode newNode(int i, boolean master) {
        Set<DiscoveryNode.Role> roles = new HashSet<>();
        if (master) {
            roles.add(DiscoveryNode.Role.MASTER);
        }
        final String prefix = master ? "master_" : "data_";
        return new DiscoveryNode(prefix + i, i + "", buildNewFakeTransportAddress(), emptyMap(), roles, Version.CURRENT);
    }

    static class SimpleFuture extends BaseFuture<Void> {
        final String description;

        SimpleFuture(String description) {
            this.description = description;
        }

        public void markAsDone() {
            set(null);
        }

        public void markAsFailed(Throwable t) {
            setException(t);
        }

        @Override
        public String toString() {
            return "future [" + description + "]";
        }
    }

    private SimpleFuture joinNodeAsync(final JoinRequest joinRequest) {
        final SimpleFuture future = new SimpleFuture("join of " + joinRequest + "]");
        logger.debug("starting {}", future);
        // clone the node before submitting to simulate an incoming join, which is guaranteed to have a new
        // disco node object serialized off the network
        coordinator.handleJoinRequest(joinRequest, new JoinHelper.JoinCallback() {
            @Override
            public void onSuccess() {
                logger.debug("{} completed", future);
                future.markAsDone();
            }

            @Override
            public void onFailure(Exception e) {
                logger.error(() -> new ParameterizedMessage("unexpected error for {}", future), e);
                future.markAsFailed(e);
            }
        });
        return future;
    }

    private void joinNode(final JoinRequest joinRequest) {
        FutureUtils.get(joinNodeAsync(joinRequest));
    }

    private void joinNodeAndRun(final JoinRequest joinRequest) {
        SimpleFuture fut = joinNodeAsync(joinRequest);
        deterministicTaskQueue.runAllTasks(random());
        assertTrue(fut.isDone());
        FutureUtils.get(fut);
    }

    public void testJoinWithHigherTermElectsLeader() {
        DiscoveryNode node0 = newNode(0, true);
        DiscoveryNode node1 = newNode(1, true);
        setupFakeMasterServiceAndCoordinator(1, initialState(false, node0, 1, 1,
            new VotingConfiguration(Collections.singleton(randomFrom(node0, node1).getId()))));
        assertFalse(isLocalNodeElectedMaster());
        joinNodeAndRun(new JoinRequest(node1, Optional.of(new Join(node1, node0, 2, 1, 1))));
        assertTrue(isLocalNodeElectedMaster());
    }

    public void testJoinWithHigherTermButBetterStateGetsRejected() {
        DiscoveryNode node0 = newNode(0, true);
        DiscoveryNode node1 = newNode(1, true);
        setupFakeMasterServiceAndCoordinator(1, initialState(false, node0, 1, 1,
            new VotingConfiguration(Collections.singleton(node1.getId()))));
        assertFalse(isLocalNodeElectedMaster());
        expectThrows(CoordinationStateRejectedException.class,
            () -> joinNodeAndRun(new JoinRequest(node1, Optional.of(new Join(node1, node0, 2, 2, 2)))));
        assertFalse(isLocalNodeElectedMaster());
    }

    public void testJoinWithHigherTermButBetterStateStillElectsMasterThroughSelfJoin() {
        DiscoveryNode node0 = newNode(0, true);
        DiscoveryNode node1 = newNode(1, true);
        setupFakeMasterServiceAndCoordinator(1, initialState(false, node0, 1, 1,
            new VotingConfiguration(Collections.singleton(node0.getId()))));
        assertFalse(isLocalNodeElectedMaster());
        joinNodeAndRun(new JoinRequest(node1, Optional.of(new Join(node1, node0, 2, 2, 2))));
        assertTrue(isLocalNodeElectedMaster());
    }

    public void testJoinElectedLeader() {
        DiscoveryNode node0 = newNode(0, true);
        DiscoveryNode node1 = newNode(1, true);
        setupFakeMasterServiceAndCoordinator(1, initialState(false, node0, 1, 1,
            new VotingConfiguration(Collections.singleton(node0.getId()))));
        assertFalse(isLocalNodeElectedMaster());
        joinNodeAndRun(new JoinRequest(node0, Optional.of(new Join(node0, node0, 2, 1, 1))));
        assertTrue(isLocalNodeElectedMaster());
        assertFalse(clusterStateHasNode(node1));
        joinNodeAndRun(new JoinRequest(node1, Optional.of(new Join(node1, node0, 2, 1, 1))));
        assertTrue(isLocalNodeElectedMaster());
        assertTrue(clusterStateHasNode(node1));
    }

    public void testJoinAccumulation() {
        DiscoveryNode node0 = newNode(0, true);
        DiscoveryNode node1 = newNode(1, true);
        setupFakeMasterServiceAndCoordinator(1, initialState(false, node0, 1, 1,
            new VotingConfiguration(Collections.singleton(node1.getId()))));
        assertFalse(isLocalNodeElectedMaster());
        SimpleFuture fut = joinNodeAsync(new JoinRequest(node0, Optional.of(new Join(node0, node0, 2, 1, 1))));
        deterministicTaskQueue.runAllTasks(random());
        assertFalse(fut.isDone());
        assertFalse(isLocalNodeElectedMaster());
        joinNodeAndRun(new JoinRequest(node1, Optional.of(new Join(node1, node0, 2, 1, 1))));
        assertTrue(isLocalNodeElectedMaster());
        assertTrue(clusterStateHasNode(node1));
        FutureUtils.get(fut);
    }

    public void testJoinFollowerFails() {
        DiscoveryNode node0 = newNode(0, true);
        DiscoveryNode node1 = newNode(1, true);
        setupFakeMasterServiceAndCoordinator(1, initialState(false, node0, 1, 1,
            new VotingConfiguration(Collections.singleton(node0.getId()))));
        coordinator.coordinationState.get().handleStartJoin(new StartJoinRequest(node1, 2));
        synchronized (coordinator.mutex) {
            coordinator.becomeFollower("test", node1);
        }
        assertFalse(isLocalNodeElectedMaster());
        assertThat(expectThrows(CoordinationStateRejectedException.class,
            () -> joinNodeAndRun(new JoinRequest(node1, Optional.of(new Join(node1, node0, 2, 1, 1))))).getMessage(),
            containsString("join target is a follower"));
        assertFalse(isLocalNodeElectedMaster());
    }

    public void testBecomeFollowerFailsPendingJoin() {
        DiscoveryNode node0 = newNode(0, true);
        DiscoveryNode node1 = newNode(1, true);
        setupFakeMasterServiceAndCoordinator(1, initialState(false, node0, 1, 1,
            new VotingConfiguration(Collections.singleton(node1.getId()))));
        SimpleFuture fut = joinNodeAsync(new JoinRequest(node0, Optional.of(new Join(node0, node0, 2, 1, 1))));
        deterministicTaskQueue.runAllTasks(random());
        assertFalse(fut.isDone());
        assertFalse(isLocalNodeElectedMaster());
        synchronized (coordinator.mutex) {
            coordinator.becomeFollower("test", node1);
        }
        assertFalse(isLocalNodeElectedMaster());
        assertThat(expectThrows(CoordinationStateRejectedException.class,
            () -> FutureUtils.get(fut)).getMessage(),
            containsString("following another master"));
        assertFalse(isLocalNodeElectedMaster());
    }

    public void testConcurrentJoining() {
        List<DiscoveryNode> nodes = IntStream.rangeClosed(1, randomIntBetween(2, 5))
            .mapToObj(nodeId -> newNode(nodeId, true)).collect(Collectors.toList());

        VotingConfiguration votingConfiguration = new VotingConfiguration(
            randomSubsetOf(randomIntBetween(1, nodes.size()), nodes).stream().map(DiscoveryNode::getId).collect(Collectors.toSet()));

        logger.info("Voting configuration: {}", votingConfiguration);

        DiscoveryNode localNode = nodes.get(0);
        setupRealMasterServiceAndCoordinator(1, initialState(false, localNode, 1, 1, votingConfiguration));

        // we need at least a quorum of voting nodes with a correct term and worse state
        List<DiscoveryNode> successfulNodes;
        do {
            successfulNodes = randomSubsetOf(nodes);
        } while (votingConfiguration.hasQuorum(successfulNodes.stream().map(DiscoveryNode::getId).collect(Collectors.toList()))
            == false);

        logger.info("Successful voting nodes: {}", successfulNodes);

        List<JoinRequest> correctJoinRequests = successfulNodes.stream().map(
            node -> new JoinRequest(node, Optional.of(new Join(node, localNode, 2, 1, 1)))).collect(Collectors.toList());

        List<DiscoveryNode> possiblyUnsuccessfulNodes = new ArrayList<>(nodes);
        possiblyUnsuccessfulNodes.removeAll(successfulNodes);

        logger.info("Possibly unsuccessful voting nodes: {}", possiblyUnsuccessfulNodes);

        List<JoinRequest> possiblyFailingJoinRequests = possiblyUnsuccessfulNodes.stream().map(node -> {
            if (randomBoolean()) {
                // a correct request
                return new JoinRequest(node, Optional.of(new Join(node, localNode, 2, 1, 1)));
            } else if (randomBoolean()) {
                // term too low
                return new JoinRequest(node, Optional.of(new Join(node, localNode, 1, 1, 1)));
            } else {
                // better state
                return new JoinRequest(node, Optional.of(new Join(node, localNode, 2, 2, 2)));
            }
        }).collect(Collectors.toList());

        // duplicate some requests, which will be unsuccessful
        possiblyFailingJoinRequests.addAll(randomSubsetOf(possiblyFailingJoinRequests));

        CyclicBarrier barrier = new CyclicBarrier(correctJoinRequests.size() + possiblyFailingJoinRequests.size() + 1);
        List<Thread> threads = new ArrayList<>();
        threads.add(new Thread(() -> {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                throw new RuntimeException(e);
            }
            for (int i = 0; i < 30; i++) {
                coordinator.invariant();
            }
        }));
        threads.addAll(correctJoinRequests.stream().map(joinRequest -> new Thread(
            () -> {
                try {
                    barrier.await();
                } catch (InterruptedException | BrokenBarrierException e) {
                    throw new RuntimeException(e);
                }
                joinNode(joinRequest);
            })).collect(Collectors.toList()));
        threads.addAll(possiblyFailingJoinRequests.stream().map(joinRequest -> new Thread(() -> {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                throw new RuntimeException(e);
            }
            try {
                joinNode(joinRequest);
            } catch (CoordinationStateRejectedException ignore) {
                // ignore
            }
        })).collect(Collectors.toList()));

        threads.forEach(Thread::start);
        threads.forEach(t -> {
            try {
                t.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        assertTrue(MasterServiceTests.discoveryState(masterService).nodes().isLocalNodeElectedMaster());
        successfulNodes.forEach(node -> assertTrue(clusterStateHasNode(node)));
    }

    private boolean isLocalNodeElectedMaster() {
        return MasterServiceTests.discoveryState(masterService).nodes().isLocalNodeElectedMaster();
    }

    private boolean clusterStateHasNode(DiscoveryNode node) {
        return node.equals(MasterServiceTests.discoveryState(masterService).nodes().get(node.getId()));
    }
}
