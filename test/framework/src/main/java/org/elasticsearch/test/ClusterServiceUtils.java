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
package org.elasticsearch.test;

import org.apache.logging.log4j.core.util.Throwables;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.LocalClusterUpdateTask;
import org.elasticsearch.cluster.NodeConnectionsService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.AbstractClusterTaskExecutor;
import org.elasticsearch.cluster.service.ClusterApplier;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.discovery.DiscoveryService;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.Discovery.AckListener;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.discovery.DiscoveryUpdateTask;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiConsumer;

import static junit.framework.TestCase.fail;

public class ClusterServiceUtils {

    public static DiscoveryService createDiscoveryService(ThreadPool threadPool) {
        DiscoveryNode discoveryNode = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), Collections.emptyMap(),
            new HashSet<>(Arrays.asList(DiscoveryNode.Role.values())),Version.CURRENT);
        return createDiscoveryService(threadPool, discoveryNode);
    }

    public static DiscoveryService createDiscoveryService(ThreadPool threadPool, DiscoveryNode localNode) {
        ClusterApplier applier = (s, c, l) -> l.clusterStateProcessed(s, null, null);
        DiscoveryService discoveryService = new DiscoveryService(Settings.builder().put("cluster.name", "ClusterServiceTests").build(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadPool, () -> localNode, applier);
        discoveryService.setNodeConnectionsService(new NodeConnectionsService(Settings.EMPTY, null, null) {
            @Override
            public void connectToNodes(Iterable<DiscoveryNode> discoveryNodes) {
                // skip
            }

            @Override
            public void disconnectFromNodesExcept(Iterable<DiscoveryNode> nodesToKeep) {
                // skip
            }
        });
        discoveryService.setClusterStatePublisher((event, ackListener) -> {});
        discoveryService.setDiscoverySettings(new DiscoverySettings(Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)));
        discoveryService.start();
        final DiscoveryNodes.Builder nodes = DiscoveryNodes.builder(discoveryService.state().nodes());
        nodes.masterNodeId(discoveryService.localNode().getId());
        setState(discoveryService, ClusterState.builder(discoveryService.state()).nodes(nodes).build());
        return discoveryService;
    }

    public static void setState(AbstractClusterTaskExecutor executor, ClusterState clusterState) {
        CountDownLatch latch = new CountDownLatch(1);
        executor.submitStateUpdateTask("test setting state", new DiscoveryUpdateTask() {
            @Override
            public ClusterTasksResult<LocalClusterUpdateTask> execute(ClusterState currentState) throws Exception {
                // make sure we increment versions as listener may depend on it for change
                return newState(ClusterState.builder(clusterState).version(currentState.version() + 1).build());
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                latch.countDown();
            }

            @Override
            public void onFailure(String source, Exception e) {
                fail("unexpected exception" + e);
            }
        });
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new ElasticsearchException("unexpected interruption", e);
        }
    }

    public static ClusterService createClusterService(ThreadPool threadPool) {
        DiscoveryNode discoveryNode = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), Collections.emptyMap(),
                                                           new HashSet<>(Arrays.asList(DiscoveryNode.Role.values())),Version.CURRENT);
        return createClusterService(threadPool, discoveryNode);
    }

    public static ClusterService createClusterService(ThreadPool threadPool, DiscoveryNode localNode) {
        ClusterService clusterService = new ClusterService(Settings.builder().put("cluster.name", "ClusterServiceTests").build(),
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                threadPool, () -> localNode);
        clusterService.setNodeConnectionsService(new NodeConnectionsService(Settings.EMPTY, null, null) {
            @Override
            public void connectToNodes(Iterable<DiscoveryNode> discoveryNodes) {
                // skip
            }

            @Override
            public void disconnectFromNodesExcept(Iterable<DiscoveryNode> nodesToKeep) {
                // skip
            }
        });
        clusterService.getDiscoveryService().setClusterStatePublisher(
            createClusterStatePublisher(clusterService.getClusterApplierService()));
        clusterService.getDiscoveryService().setDiscoverySettings(new DiscoverySettings(Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)));
        clusterService.start();
        final DiscoveryNodes.Builder nodes = DiscoveryNodes.builder(clusterService.state().nodes());
        nodes.masterNodeId(clusterService.localNode().getId());
        setState(clusterService, ClusterState.builder(clusterService.state()).nodes(nodes));
        return clusterService;
    }

    public static BiConsumer<ClusterChangedEvent, AckListener> createClusterStatePublisher(ClusterApplier clusterApplier) {
        return (event, ackListener) -> {
            CountDownLatch latch = new CountDownLatch(1);
            clusterApplier.submitStateUpdateTask("mock_publish_to_self[" + event.source() + "]", event.state(),
                new ClusterStateTaskListener() {

                @Override
                public void onFailure(String source, Exception e) {
                    latch.countDown();
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    latch.countDown();
                }
            });
//            if (clusterApplierService.lifecycleState().equals(Lifecycle.State.STOPPED) == false) {
//                // if cluster service is stopped, there is no point in waiting
                try {
                    latch.await();
                } catch (Exception e) {
                    Throwables.rethrow(e);
                }
//            }
        };
    }

    public static ClusterService createClusterService(ClusterState initialState, ThreadPool threadPool) {
        ClusterService clusterService = createClusterService(threadPool);
        setState(clusterService, initialState);
        return clusterService;
    }

    public static void setState(ClusterService clusterService, ClusterState.Builder clusterStateBuilder) {
        setState(clusterService, clusterStateBuilder.build());
    }

    public static void setState(ClusterService clusterService, ClusterState clusterState) {
        setState(clusterService.getDiscoveryService(), clusterState);
    }
}
