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
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.NodeConnectionsService;
import org.elasticsearch.cluster.ack.AckedRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.discovery.zen2.ConsensusStateTests.clusterState;
import static org.elasticsearch.discovery.zen2.ConsensusStateTests.setValue;
import static org.elasticsearch.discovery.zen2.ConsensusStateTests.value;
import static org.elasticsearch.discovery.zen2.LegislatorTransport.registerTransportActions;
import static org.hamcrest.Matchers.equalTo;

public class ConcurrentLegislatorTests extends ESTestCase {

    public void testSimpleClusterFormation() throws Exception {
        final int numClusterNodes = randomIntBetween(1, 10);
        final List<ClusterNode> clusterNodes = new ArrayList<>(numClusterNodes);
        for (int i = 0; i < numClusterNodes; i++) {
            clusterNodes.add(new ClusterNode("node" + i));
        }

        try {
            for (ClusterNode node1 : clusterNodes) {
                for (ClusterNode node2 : clusterNodes) {
                    if (node1 != node2) {
                        node1.mockTransportService.connectToNode(node2.localNode);
                    }
                }
            }

            List<DiscoveryNode> allNodes = clusterNodes.stream().map(node -> node.localNode).collect(Collectors.toList());

            Set<String> votingNodes = randomSubsetOf(allNodes).stream().map(DiscoveryNode::getId).collect(Collectors.toSet());
            if (votingNodes.isEmpty()) {
                votingNodes = Collections.singleton(allNodes.get(0).getId());
            }
            ClusterState.VotingConfiguration initialConfig = new ClusterState.VotingConfiguration(votingNodes);

            clusterNodes.forEach(node -> node.initialise(initialConfig, () -> allNodes));

            assertBusy(() -> clusterNodes.forEach(node -> {
                assertThat(node.clusterService.state().nodes().getSize(), equalTo(numClusterNodes));
            }));

            DiscoveryNode masterNode = clusterNodes.get(0).clusterService.state().nodes().getMasterNode();
            ClusterNode master = clusterNodes.stream().filter(n -> n.localNode.equals(masterNode)).findAny().get();

            PlainActionFuture<Void> fut = new PlainActionFuture<>();
            master.clusterService.submitStateUpdateTask("change value to 13", new AckedClusterStateUpdateTask<Void>(
                new AckedRequest() {
                    @Override
                    public TimeValue ackTimeout() {
                        return TimeValue.timeValueMinutes(1);
                    }

                    @Override
                    public TimeValue masterNodeTimeout() {
                        return TimeValue.timeValueMinutes(1);
                    }
                }, fut) {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        return setValue(currentState, 13);
                    }

                    @Override
                    protected Void newResponse(boolean acknowledged) {
                        return null;
                    }
                });

            fut.get();

            clusterNodes.forEach(node -> assertThat(value(node.clusterService.state()), equalTo(13L)));
        } finally {
            IOUtils.close(clusterNodes);
        }
    }

    class ClusterNode implements Closeable {
        private final Settings settings;
        private final DiscoveryNode localNode;
        private final ThreadPool threadPool;
        private final MockTransportService mockTransportService;
        private Legislator legislator;
        final ClusterService clusterService;

        ClusterNode(String name) {
            settings = Settings.builder()
                .put("node.name", name)
                .build();

            threadPool = new ThreadPool(settings);

            mockTransportService = MockTransportService.createNewService(settings, Version.CURRENT,
                threadPool, null);
            mockTransportService.start();
            mockTransportService.acceptIncomingRequests();

            localNode = mockTransportService.getLocalNode();

            ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
            clusterService = new ClusterService(settings, clusterSettings, threadPool, Collections.emptyMap());
        }

        void initialise(ClusterState.VotingConfiguration initialConfiguration, Supplier<List<DiscoveryNode>> nodeSupplier) {
            ConsensusState.BasePersistedState persistedState = new ConsensusState.BasePersistedState(0L,
                clusterState(0L, 0L, localNode, initialConfiguration, initialConfiguration, 42L));

            Legislator.FutureExecutor futureExecutor = (delay, description, task) ->
                threadPool.schedule(delay, ThreadPool.Names.GENERIC, task);

            Legislator.Transport transport = new LegislatorTransport(mockTransportService);

            legislator = new Legislator(settings, persistedState, transport, clusterService.getMasterService(),
                ESAllocationTestCase.createAllocationService(), localNode,
                System::nanoTime, futureExecutor, nodeSupplier, clusterService.getClusterApplierService(), new Random());

            clusterService.getMasterService().setClusterStatePublisher(legislator::publish);

            clusterService.setNodeConnectionsService(new NodeConnectionsService(Settings.EMPTY, null, null) {
                @Override
                public void connectToNodes(DiscoveryNodes discoveryNodes) {
                    // skip
                }

                @Override
                public void disconnectFromNodesExcept(DiscoveryNodes nodesToKeep) {
                    // skip
                }
            });

            legislator.start();
            clusterService.start();

            registerTransportActions(mockTransportService, legislator);
            legislator.startInitialJoin();
        }

        public void close() {
            IOUtils.closeWhileHandlingException(mockTransportService, clusterService);
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

}
