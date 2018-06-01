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
        final int numVotingNodes = randomIntBetween(1, numClusterNodes);
        final List<ClusterNode> clusterNodes = new ArrayList<>(numClusterNodes);

        for (int i = 0; i < numClusterNodes; i++) {
            clusterNodes.add(
                new ClusterNode("node" + i,
                    () -> new ClusterState.VotingConfiguration(clusterNodes.stream()
                        .map(node -> node.mockTransportService.getLocalNode().getId())
                        .limit(numVotingNodes).collect(Collectors.toSet())),
                    () -> clusterNodes.stream().map(node -> node.mockTransportService.getLocalNode()).collect(Collectors.toList())));
        }

        try {
            clusterNodes.forEach(node -> node.mockTransportService.start());

            clusterNodes.forEach(node -> node.start());

            for (ClusterNode node1 : clusterNodes) {
                for (ClusterNode node2 : clusterNodes) {
                    if (node1 != node2) {
                        node1.mockTransportService.connectToNode(node2.mockTransportService.getLocalNode());
                    }
                }
            }

            assertBusy(() -> clusterNodes.forEach(node -> {
                assertThat(node.clusterService.state().nodes().getSize(), equalTo(numClusterNodes));
            }));

            DiscoveryNode masterNode = clusterNodes.get(0).clusterService.state().nodes().getMasterNode();
            ClusterNode master = clusterNodes.stream()
                .filter(n -> n.mockTransportService.getLocalNode().equals(masterNode)).findAny().get();

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
        private final ThreadPool threadPool;
        private final MockTransportService mockTransportService;
        private final Legislator legislator;
        final ClusterService clusterService;

        ClusterNode(String name, Supplier<ClusterState.VotingConfiguration> initialConfigurationSupplier,
                    Supplier<List<DiscoveryNode>> nodeSupplier) {
            settings = Settings.builder()
                .put("node.name", name)
                .build();

            threadPool = new ThreadPool(settings);

            mockTransportService = MockTransportService.createNewService(settings, Version.CURRENT,
                threadPool, null);

            ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
            clusterService = new ClusterService(settings, clusterSettings, threadPool, Collections.emptyMap());
            clusterService.setNodeConnectionsService(new NodeConnectionsService(settings, threadPool, mockTransportService));

            Legislator.FutureExecutor futureExecutor = (delay, description, task) ->
                threadPool.schedule(delay, ThreadPool.Names.GENERIC, task);

            Legislator.Transport transport = new LegislatorTransport(mockTransportService);

            Supplier<ConsensusState.PersistedState> persistedStateSupplier = () -> {
                final ClusterState.VotingConfiguration initialConfiguration = initialConfigurationSupplier.get();
                return new ConsensusState.BasePersistedState(0L,
                    clusterState(0L, 0L, mockTransportService.getLocalNode(), initialConfiguration, initialConfiguration, 42L));
            };
            legislator = new Legislator(settings, persistedStateSupplier, transport, clusterService.getMasterService(),
                ESAllocationTestCase.createAllocationService(),
                System::nanoTime, futureExecutor, nodeSupplier, clusterService.getClusterApplierService(), new Random());

            registerTransportActions(mockTransportService, legislator);

            clusterService.getMasterService().setClusterStatePublisher(legislator::publish);
        }

        void start() {
            legislator.start();
            clusterService.start();
            mockTransportService.acceptIncomingRequests();
            legislator.startInitialJoin();
        }

        public void close() {
            IOUtils.closeWhileHandlingException(mockTransportService, clusterService);
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

}
