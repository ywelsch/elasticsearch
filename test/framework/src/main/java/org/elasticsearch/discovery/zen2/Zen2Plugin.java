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

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterApplier;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.discovery.zen.UnicastHostsProvider;
import org.elasticsearch.plugins.DiscoveryPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.discovery.MockUncasedHostProvider;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.discovery.zen.UnicastZenPing.DISCOVERY_ZEN_PING_UNICAST_HOSTS_SETTING;
import static org.elasticsearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;

public class Zen2Plugin extends Plugin implements DiscoveryPlugin {

    private final Settings settings;

    private final SetOnce<MockUncasedHostProvider> unicastHostProvider = new SetOnce<>();

    public Zen2Plugin(Settings settings) {
        this.settings = settings;
    }

    @Override
    public Map<String, Supplier<Discovery>> getDiscoveryTypes(ThreadPool threadPool, TransportService transportService,
                                                              NamedWriteableRegistry namedWriteableRegistry,
                                                              MasterService masterService,
                                                              ClusterApplier clusterApplier,
                                                              ClusterSettings clusterSettings,
                                                              UnicastHostsProvider hostsProvider,
                                                              AllocationService allocationService) {
        return Collections.singletonMap("zen2",
            () -> {
                final LegislatorTransport legislatorTransport = new LegislatorTransport(transportService);
                final Legislator.FutureExecutor futureExecutor = (delay, description, task) ->
                    threadPool.schedule(delay, ThreadPool.Names.GENERIC, task);

                // TODO: where to get the initial configuration
                Supplier<ConsensusState.PersistedState> persistedStateSupplier = () -> {
                    List<DiscoveryNode> discoveryNodes = hostsProvider.buildDynamicNodes();
                    final ClusterState.VotingConfiguration initialConfiguration =
                        new ClusterState.VotingConfiguration(discoveryNodes.stream().map(DiscoveryNode::getId).collect(Collectors.toSet()));
                    ClusterState initialState = clusterApplier.newClusterStateBuilder()
                        .blocks(ClusterBlocks.builder()
                            .addGlobalBlock(STATE_NOT_RECOVERED_BLOCK)
                            .addGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_SETTING.get(settings)))
                        .nodes(DiscoveryNodes.builder().add(transportService.getLocalNode())
                            .localNodeId(transportService.getLocalNode().getId()))
                        .lastAcceptedConfiguration(initialConfiguration)
                        .lastCommittedConfiguration(initialConfiguration)
                        .build();
                    return new ConsensusState.BasePersistedState(0L, initialState);
                };

                final Legislator legislator = new Legislator(
                    settings,
                    persistedStateSupplier,
                    legislatorTransport,
                    masterService,
                    allocationService,
                    System::nanoTime,
                    futureExecutor,
                    () -> {
                        List<DiscoveryNode> discoveryNodes = hostsProvider.buildDynamicNodes();
                        discoveryNodes.forEach(node -> {
                            try {
                                transportService.connectToNode(node);
                            } catch (Exception e) {
                                // ignore
                            }
                        });
                        return discoveryNodes;
                    }
                    , // TODO: what should this be?
                    clusterApplier,
                    new Random()
                    );
                LegislatorTransport.registerTransportActions(transportService, legislator);
                return legislator;
            });
    }

    @Override
    public Map<String, Supplier<UnicastHostsProvider>> getZenHostsProviders(TransportService transportService,
                                                                            NetworkService networkService) {
        final Supplier<UnicastHostsProvider> supplier;
        supplier = () -> {
            unicastHostProvider.set(
                new MockUncasedHostProvider(transportService::getLocalNode, ClusterName.CLUSTER_NAME_SETTING.get(settings))
            );
            return unicastHostProvider.get();
        };
        return Collections.singletonMap("zen2", supplier);
    }

    @Override
    public Settings additionalSettings() {
        return Settings.builder()
            .put(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey(), "zen2")
            .put(DiscoveryModule.DISCOVERY_HOSTS_PROVIDER_SETTING.getKey(), "zen2")
            .putList(DISCOVERY_ZEN_PING_UNICAST_HOSTS_SETTING.getKey())
            .build();
    }

    @Override
    public void close() throws IOException {
        super.close();
        if (unicastHostProvider.get() != null) {
            unicastHostProvider.get().close();
        }
    }
}
