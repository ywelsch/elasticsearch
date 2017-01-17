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
package org.elasticsearch.cluster.service;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.NodeConnectionsService;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.Matchers.is;

public class ClusterApplierServiceTests extends AbstractClusterTaskExecutorTestCase<ClusterApplierServiceTests.TimedClusterApplierService> {

    @Override
    protected TimedClusterApplierService createClusterTaskExecutor() throws InterruptedException {
        return createTimedClusterService(true);
    }

    TimedClusterApplierService createTimedClusterService(boolean makeMaster) throws InterruptedException {
        DiscoveryNode localNode = new DiscoveryNode("node1", buildNewFakeTransportAddress(), emptyMap(),
            emptySet(), Version.CURRENT);
        TimedClusterApplierService timedClusterApplierService = new TimedClusterApplierService(Settings.builder().put("cluster.name",
            "ClusterApplierServiceTests").build(), new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadPool, () -> localNode);
        timedClusterApplierService.setNodeConnectionsService(new NodeConnectionsService(Settings.EMPTY, null, null) {
            @Override
            public void connectToNodes(Iterable<DiscoveryNode> discoveryNodes) {
                // skip
            }

            @Override
            public void disconnectFromNodesExcept(Iterable<DiscoveryNode> nodesToKeep) {
                // skip
            }
        });
        timedClusterApplierService.start();
        ClusterState state = timedClusterApplierService.state();
        final DiscoveryNodes nodes = state.nodes();
        final DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(nodes)
            .masterNodeId(makeMaster ? nodes.getLocalNodeId() : null);
        state = ClusterState.builder(state).blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
            .nodes(nodesBuilder).build();
        setState(timedClusterApplierService, state);
        return timedClusterApplierService;
    }

    @TestLogging("org.elasticsearch.cluster.service:TRACE") // To ensure that we log cluster state events on TRACE level
    public void testClusterStateUpdateLogging() throws Exception {
        MockLogAppender mockAppender = new MockLogAppender();
        mockAppender.start();
        mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                        "test1",
                        clusterTaskExecutor.getClass().getName(),
                        Level.DEBUG,
                        "*processing [test1]: took [1s] no change in cluster_state"));
        mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                        "test2",
                        clusterTaskExecutor.getClass().getName(),
                        Level.TRACE,
                        "*failed to execute cluster state update in [2s]*"));

        Logger clusterLogger = Loggers.getLogger("org.elasticsearch.cluster.service");
        Loggers.addAppender(clusterLogger, mockAppender);
        try {
            final CountDownLatch latch = new CountDownLatch(4);
            clusterTaskExecutor.currentTimeOverride = System.nanoTime();
            clusterTaskExecutor.submitStateUpdateTask("test1", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    clusterTaskExecutor.currentTimeOverride += TimeValue.timeValueSeconds(1).nanos();
                    return currentState;
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    latch.countDown();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    fail();
                }
            });
            clusterTaskExecutor.submitStateUpdateTask("test2", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    clusterTaskExecutor.currentTimeOverride += TimeValue.timeValueSeconds(2).nanos();
                    throw new IllegalArgumentException("Testing handling of exceptions in the cluster state task");
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    fail();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    latch.countDown();
                }
            });
            clusterTaskExecutor.submitStateUpdateTask("test3", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    clusterTaskExecutor.currentTimeOverride += TimeValue.timeValueSeconds(3).nanos();
                    return ClusterState.builder(currentState).incrementVersion().build();
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    latch.countDown();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    fail();
                }
            });
            // Additional update task to make sure all previous logging made it to the loggerName
            // We don't check logging for this on since there is no guarantee that it will occur before our check
            clusterTaskExecutor.submitStateUpdateTask("test4", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return currentState;
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    latch.countDown();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    fail();
                }
            });
            latch.await();
        } finally {
            Loggers.removeAppender(clusterLogger, mockAppender);
            mockAppender.stop();
        }
        mockAppender.assertAllExpectationsMatched();
    }

    @TestLogging("org.elasticsearch.cluster.service:WARN") // To ensure that we log cluster state events on WARN level
    public void testLongClusterStateUpdateLogging() throws Exception {
        MockLogAppender mockAppender = new MockLogAppender();
        mockAppender.start();
        mockAppender.addExpectation(
                new MockLogAppender.UnseenEventExpectation(
                        "test1 shouldn't see because setting is too low",
                        clusterTaskExecutor.getClass().getName(),
                        Level.WARN,
                        "*cluster state update task [test1] took [*] above the warn threshold of *"));
        mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                        "test2",
                        clusterTaskExecutor.getClass().getName(),
                        Level.WARN,
                        "*cluster state update task [test2] took [32s] above the warn threshold of *"));
        mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                        "test3",
                        clusterTaskExecutor.getClass().getName(),
                        Level.WARN,
                        "*cluster state update task [test3] took [33s] above the warn threshold of *"));
        mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                        "test4",
                        clusterTaskExecutor.getClass().getName(),
                        Level.WARN,
                        "*cluster state update task [test4] took [34s] above the warn threshold of *"));

        Logger clusterLogger = Loggers.getLogger("org.elasticsearch.cluster.service");
        Loggers.addAppender(clusterLogger, mockAppender);
        try {
            final CountDownLatch latch = new CountDownLatch(5);
            final CountDownLatch processedFirstTask = new CountDownLatch(1);
            clusterTaskExecutor.currentTimeOverride = System.nanoTime();
            clusterTaskExecutor.submitStateUpdateTask("test1", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    clusterTaskExecutor.currentTimeOverride += TimeValue.timeValueSeconds(1).nanos();
                    return currentState;
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    latch.countDown();
                    processedFirstTask.countDown();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    fail();
                }
            });

            processedFirstTask.await();
            clusterTaskExecutor.submitStateUpdateTask("test2", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    clusterTaskExecutor.currentTimeOverride += TimeValue.timeValueSeconds(32).nanos();
                    throw new IllegalArgumentException("Testing handling of exceptions in the cluster state task");
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    fail();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    latch.countDown();
                }
            });
            clusterTaskExecutor.submitStateUpdateTask("test3", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    clusterTaskExecutor.currentTimeOverride += TimeValue.timeValueSeconds(33).nanos();
                    return ClusterState.builder(currentState).incrementVersion().build();
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    latch.countDown();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    fail();
                }
            });
            clusterTaskExecutor.submitStateUpdateTask("test4", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    clusterTaskExecutor.currentTimeOverride += TimeValue.timeValueSeconds(34).nanos();
                    return currentState;
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    latch.countDown();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    fail();
                }
            });
            // Additional update task to make sure all previous logging made it to the loggerName
            // We don't check logging for this on since there is no guarantee that it will occur before our check
            clusterTaskExecutor.submitStateUpdateTask("test5", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return currentState;
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    latch.countDown();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    fail();
                }
            });
            latch.await();
        } finally {
            Loggers.removeAppender(clusterLogger, mockAppender);
            mockAppender.stop();
        }
        mockAppender.assertAllExpectationsMatched();
    }

    public void testLocalNodeMasterListenerCallbacks() throws Exception {
        TimedClusterApplierService timedClusterApplierService = createTimedClusterService(false);

        AtomicBoolean isMaster = new AtomicBoolean();
        timedClusterApplierService.addLocalNodeMasterListener(new LocalNodeMasterListener() {
            @Override
            public void onMaster() {
                isMaster.set(true);
            }

            @Override
            public void offMaster() {
                isMaster.set(false);
            }

            @Override
            public String executorName() {
                return ThreadPool.Names.SAME;
            }
        });

        ClusterState state = timedClusterApplierService.state();
        DiscoveryNodes nodes = state.nodes();
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(nodes).masterNodeId(nodes.getLocalNodeId());
        state = ClusterState.builder(state).blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK).nodes(nodesBuilder).build();
        setState(timedClusterApplierService, state);
        assertThat(isMaster.get(), is(true));

        nodes = state.nodes();
        nodesBuilder = DiscoveryNodes.builder(nodes).masterNodeId(null);
        state = ClusterState.builder(state).blocks(ClusterBlocks.builder().addGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_WRITES))
            .nodes(nodesBuilder).build();
        setState(timedClusterApplierService, state);
        assertThat(isMaster.get(), is(false));
        nodesBuilder = DiscoveryNodes.builder(nodes).masterNodeId(nodes.getLocalNodeId());
        state = ClusterState.builder(state).blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK).nodes(nodesBuilder).build();
        setState(timedClusterApplierService, state);
        assertThat(isMaster.get(), is(true));

        timedClusterApplierService.close();
    }

    public void testClusterStateApplierCantSampleClusterState() throws InterruptedException {
        AtomicReference<Throwable> error = new AtomicReference<>();
        AtomicBoolean applierCalled = new AtomicBoolean();
        clusterTaskExecutor.addStateApplier(event -> {
            try {
                applierCalled.set(true);
                clusterTaskExecutor.state();
                error.set(new AssertionError("successfully sampled state"));
            } catch (AssertionError e) {
                if (e.getMessage().contains("should not be called by a cluster state applier") == false) {
                    error.set(e);
                }
            }
        });

        CountDownLatch latch = new CountDownLatch(1);
        clusterTaskExecutor.submitStateUpdateTask("test", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                return ClusterState.builder(currentState).build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                error.compareAndSet(null, e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                latch.countDown();
            }
        });

        latch.await();
        assertNull(error.get());
        assertTrue(applierCalled.get());
    }

    static class TimedClusterApplierService extends ClusterApplierService {

        public volatile Long currentTimeOverride = null;

        public TimedClusterApplierService(Settings settings, ClusterSettings clusterSettings, ThreadPool threadPool,
                                          Supplier<DiscoveryNode> localNodeSupplier) {
            super(settings, clusterSettings, threadPool, localNodeSupplier);
        }

        @Override
        protected long currentTimeInNanos() {
            if (currentTimeOverride != null) {
                return currentTimeOverride;
            }
            return super.currentTimeInNanos();
        }
    }
}
