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
package org.elasticsearch.discovery;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.zen.ElectMasterService;
import org.elasticsearch.transport.TransportService;

import static org.elasticsearch.tribe.TribeService.BLOCKS_METADATA_SETTING;
import static org.elasticsearch.tribe.TribeService.BLOCKS_WRITE_SETTING;
import static org.elasticsearch.tribe.TribeService.TRIBE_METADATA_BLOCK;
import static org.elasticsearch.tribe.TribeService.TRIBE_WRITE_BLOCK;

/**
 * A {@link Discovery} implementation that is used by {@link org.elasticsearch.tribe.TribeService}. This implementation
 * doesn't support any clustering features. Most notably {@link #startInitialJoin()} does nothing and
 * {@link #publish(ClusterChangedEvent, AckListener)} is not supported.
 */
public class TribeDiscovery extends AbstractLifecycleComponent implements Discovery {

    private final TransportService transportService;
    private final DiscoverySettings discoverySettings;
    private final ClusterName clusterName;

    private volatile ClusterState initialState;

    @Inject
    public TribeDiscovery(Settings settings, TransportService transportService, ClusterSettings clusterSettings) {
        super(settings);
        this.clusterName = ClusterName.CLUSTER_NAME_SETTING.get(settings);
        this.transportService = transportService;
        this.discoverySettings = new DiscoverySettings(settings, clusterSettings);
    }

    @Override
    public DiscoveryNode localNode() {
        return transportService.getLocalNode();
    }

    @Override
    public String nodeDescription() {
        return clusterName.value() + "/" + localNode().getId();
    }

    @Override
    public void setAllocationService(AllocationService allocationService) {

    }

    @Override
    public void publish(ClusterChangedEvent clusterChangedEvent, AckListener ackListener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized ClusterState getInitialState() {
        if (initialState == null) {
            ClusterBlocks.Builder clusterBlocks = ClusterBlocks.builder(); // don't add master / state recovery block
            if (BLOCKS_WRITE_SETTING.get(settings)) {
                clusterBlocks.addGlobalBlock(TRIBE_WRITE_BLOCK);
            }
            if (BLOCKS_METADATA_SETTING.get(settings)) {
                clusterBlocks.addGlobalBlock(TRIBE_METADATA_BLOCK);
            }
            initialState = ClusterState.builder(clusterName)
                .nodes(DiscoveryNodes.builder().add(localNode()).localNodeId(localNode().getId()).build())
                .blocks(clusterBlocks).build();
        }
        return initialState;
    }

    @Override
    public ClusterState state() {
        return getInitialState();
    }

    @Override
    public DiscoveryStats stats() {
        return null;
    }

    @Override
    public DiscoverySettings getDiscoverySettings() {
        return discoverySettings;
    }

    @Override
    public void startInitialJoin() {

    }

    @Override
    public int getMinimumMasterNodes() {
        return ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.get(settings);
    }

    @Override
    protected void doStart() {

    }

    @Override
    protected void doStop() {

    }

    @Override
    protected void doClose() {

    }
}
