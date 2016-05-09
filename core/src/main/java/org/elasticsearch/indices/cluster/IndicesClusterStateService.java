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

package org.elasticsearch.indices.cluster;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.action.index.NodeIndexDeletedAction;
import org.elasticsearch.cluster.action.index.NodeMappingRefreshAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Callback;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexShardAlreadyExistsException;
import org.elasticsearch.index.NodeServicesProvider;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.flush.SyncedFlushService;
import org.elasticsearch.indices.recovery.RecoveryFailedException;
import org.elasticsearch.indices.recovery.RecoverySource;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.indices.recovery.RecoveryTargetService;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
public class IndicesClusterStateService extends AbstractLifecycleComponent<IndicesClusterStateService> implements ClusterStateListener {

    private final IndicesServiceProxy indicesService;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final RecoveryTargetService recoveryTargetService;
    private final ShardStateAction shardStateAction;
    private final NodeIndexDeletedAction nodeIndexDeletedAction;
    private final NodeMappingRefreshAction nodeMappingRefreshAction;
    private final NodeServicesProvider nodeServicesProvider;

    public static final ShardStateAction.Listener SHARD_STATE_ACTION_LISTENER = new ShardStateAction.Listener() {
    };

    // a list of shards that failed during recovery
    // we keep track of these shards in order to prevent repeated recovery of these shards on each cluster state update
    private final ConcurrentMap<ShardId, ShardRouting> failedShardsCache = ConcurrentCollections.newConcurrentMap();
    private final RestoreService restoreService;
    private final RepositoriesService repositoriesService;

    private final Object mutex = new Object();
    private final FailedShardHandler failedShardHandler = new FailedShardHandler();

    private final boolean sendRefreshMapping;
    private final List<IndexEventListener> buildInIndexListener;

    @Inject
    public IndicesClusterStateService(Settings settings, IndicesService indicesService, ClusterService clusterService,
                                      ThreadPool threadPool, RecoveryTargetService recoveryTargetService,
                                      ShardStateAction shardStateAction,
                                      NodeIndexDeletedAction nodeIndexDeletedAction,
                                      NodeMappingRefreshAction nodeMappingRefreshAction,
                                      RepositoriesService repositoriesService, RestoreService restoreService,
                                      SearchService searchService, SyncedFlushService syncedFlushService,
                                      RecoverySource recoverySource, NodeServicesProvider nodeServicesProvider) {
        super(settings);
        this.buildInIndexListener = Arrays.asList(recoverySource, recoveryTargetService, searchService, syncedFlushService);
        this.indicesService = new IndicesServiceProxy(logger, indicesService);
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.recoveryTargetService = recoveryTargetService;
        this.shardStateAction = shardStateAction;
        this.nodeIndexDeletedAction = nodeIndexDeletedAction;
        this.nodeMappingRefreshAction = nodeMappingRefreshAction;
        this.restoreService = restoreService;
        this.repositoriesService = repositoriesService;
        this.sendRefreshMapping = this.settings.getAsBoolean("indices.cluster.send_refresh_mapping", true);
        this.nodeServicesProvider = nodeServicesProvider;
    }

    @Override
    protected void doStart() {
        clusterService.addFirst(this);
    }

    @Override
    protected void doStop() {
        clusterService.remove(this);
    }

    @Override
    protected void doClose() {
    }

    @Override
    public void clusterChanged(final ClusterChangedEvent event) {
        if (!indicesService.changesAllowed()) {
            return;
        }

        if (!lifecycle.started()) {
            return;
        }

        synchronized (mutex) {
            Map<ShardId, ShardRouting> currentShards = indicesService.shards();
            Map<Index, IndexSettings> currentIndices = indicesService.indices();

            // we need to clean the shards and indices we have on this node, since we
            // are going to recover them again once state persistence is disabled (no master / not recovered)
            // TODO: this feels a bit hacky here, a block disables state persistence, and then we clean the allocated shards, maybe another flag in blocks?
            if (event.state().blocks().disableStatePersistence()) {
                for (Index index : currentIndices.keySet()) {
                    indicesService.removeIndex(index, "cleaning index (disabled block persistence)"); // also cleans shards
                }
                return;
            }

            updateFailedShardsCache(event, currentShards);

            Set<Index> currentDeletedIndices = applyDeletedIndices(event, currentIndices); // also deletes shards
            // remove shards from currentShards that have been removed as part of applyDeletedIndices
            for (Iterator<Map.Entry<ShardId, ShardRouting>> it = currentShards.entrySet().iterator(); it.hasNext(); ) {
                if (currentDeletedIndices.contains(it.next().getKey().getIndex())) {
                    it.remove();
                }
            }

            Set<Index> currentRemovedIndices = applyRemovedIndices(event, currentIndices); // also removes shards
            // remove shards from currentShards that have been removed as part of applyRemovedIndices
            for (Iterator<Map.Entry<ShardId, ShardRouting>> it = currentShards.entrySet().iterator(); it.hasNext(); ) {
                if (currentRemovedIndices.contains(it.next().getKey().getIndex())) {
                    it.remove();
                }
            }

            // remove shards from currentShards that have been removed as part of applyRemovedShards
            Set<ShardId> removedShards = applyRemovedShards(event, currentShards);
            for (ShardId shardId : removedShards) {
                currentShards.remove(shardId);
            }

            applyUpdateExistingIndices(event, currentIndices, currentDeletedIndices, currentRemovedIndices, currentShards); // can also fail shards, but these are then guaranteed to be in failedShardsCache

            applyNewIndices(event, currentIndices);

            applyNewOrUpdatedShards(event, currentShards);
        }
    }

    private void updateFailedShardsCache(final ClusterChangedEvent event, Map<ShardId, ShardRouting> currentShards) {
        RoutingNode localRoutingNode = event.state().getRoutingNodes().node(event.state().nodes().getLocalNodeId());
        if (localRoutingNode == null) {
            failedShardsCache.clear();
            return;
        }

        // remove items from cache which are not in our routing table anymore
        RoutingTable routingTable = event.state().routingTable();
        for (Iterator<Map.Entry<ShardId, ShardRouting>> iterator = failedShardsCache.entrySet().iterator(); iterator.hasNext(); ) {
            Map.Entry<ShardId, ShardRouting> entry = iterator.next();
            ShardId failedShardId = entry.getKey();
            ShardRouting failedShardRouting = entry.getValue();
            IndexRoutingTable indexRoutingTable = routingTable.index(failedShardId.getIndex());
            if (indexRoutingTable == null) {
                iterator.remove();
                continue;
            }
            IndexShardRoutingTable shardRoutingTable = indexRoutingTable.shard(failedShardId.id());
            if (shardRoutingTable == null) {
                iterator.remove();
                continue;
            }
            if (shardRoutingTable.assignedShards().stream().noneMatch(shr -> shr.isSameAllocation(failedShardRouting))) {
                iterator.remove();
            }
        }

        DiscoveryNodes nodes = event.state().nodes();
        for (ShardRouting shard : localRoutingNode) {
            if (failedShardsCache.containsKey(shard.shardId())) {
                if (nodes.getMasterNode() != null) {
                    String message = "master " + nodes.getMasterNode() + " has not removed shard, but shard has previous failed. resending shard failure";
                    logger.trace("[{}] re-sending failed shard [{}], reason [{}]", shard.shardId(), shard, message);
                    shardStateAction.shardFailed(shard, shard, message, null, SHARD_STATE_ACTION_LISTENER);
                }
            } else {
                if (shard.initializing() == false && currentShards.containsKey(shard.shardId()) == false) {
                    // the master thinks we are active, but we don't have this shard at all, mark it as failed
                    sendFailShard(shard, "master [" + nodes.getMasterNode() + "] marked shard as active, but shard has not been created, mark shard as failed", null);
                    // in case shard is initializing, we expect it to be a shard that is to be allocated to this node
                }
            }
        }
    }

    /**
     * Deletes indices (with shard data) for indices that have been explicitly deleted by the user.
     *
     * @param event cluster change event
     * @param currentIndices indices that are currently loaded by indicesService
     * @return set of indices that were deleted as part of this method
     */
    private Set<Index> applyDeletedIndices(final ClusterChangedEvent event, Map<Index, IndexSettings> currentIndices) {
        final ClusterState previousState = event.previousState();
        final String localNodeId = event.state().nodes().getLocalNodeId();
        assert localNodeId != null;

        Set<Index> currentDeletedIndices = new HashSet<>();
        for (Index index : event.indicesDeleted()) {
            logger.debug("{} cleaning index, no longer part of the metadata", index);
            IndexSettings indexSettings = currentIndices.get(index);
            if (indexSettings != null) {
                currentDeletedIndices.add(index);
                indicesService.deleteIndex(index, "index no longer part of the metadata");
            } else if (previousState.metaData().hasIndex(index.getName())) {
                // The deleted index was part of the previous cluster state, but not loaded on the local node
                final IndexMetaData metaData = previousState.metaData().index(index);
                indexSettings = new IndexSettings(metaData, settings);
                indicesService.deleteUnassignedIndex("deleted index was not assigned to local node", metaData, event.state());
            } else {
                // The previous cluster state's metadata also does not contain the index,
                // which is what happens on node startup when an index was deleted while the
                // node was not part of the cluster.  In this case, try reading the index
                // metadata from disk.  If its not there, there is nothing to delete.
                // First, though, verify the precondition for applying this case by
                // asserting that the previous cluster state is not initialized/recovered.
                assert previousState.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK);
                final IndexMetaData metaData = indicesService.verifyIndexIsDeleted(index, event.state());
                if (metaData != null) {
                    indexSettings = new IndexSettings(metaData, settings);
                } else {
                    indexSettings = null;
                }
            }
            // indexSettings can only be null if there was no IndexService and no metadata existed
            // on disk for this index, so it won't need to go through the node deleted action anyway
            if (indexSettings != null) {
                try {
                    nodeIndexDeletedAction.nodeIndexDeleted(event.state(), index, indexSettings, localNodeId);
                } catch (Exception e) {
                    logger.debug("failed to send to master index {} deleted event", e, index);
                }
            }
        }

        for (Index index : currentIndices.keySet()) {
            if (currentDeletedIndices.contains(index) == false) {
                IndexMetaData indexMetaData = event.state().metaData().index(index);
                if (indexMetaData == null) {
                    assert false : "index" + index + " exists locally, doesn't have a metadata but is not part "
                        + " of the delete index list. \nprevious state: " + event.previousState().prettyPrint()
                        + "\n current state:\n" + event.state().prettyPrint();
                    logger.warn("[{}] isn't part of metadata but is part of in memory structures. removing", index);
                    indicesService.deleteIndex(index, "isn't part of metadata (explicit check)");
                }
            }
        }

        return currentDeletedIndices;
    }

    /**
     * Removes indices that have no shards in routing nodes. This does not delete the shard data.
     *
     * @param event cluster change event
     * @param currentIndices indices that are currently loaded by indicesService
     * @return set of indices that were removed as part of this method
     */
    private Set<Index> applyRemovedIndices(final ClusterChangedEvent event, Map<Index, IndexSettings> currentIndices) {
        final String localNodeId = event.state().nodes().getLocalNodeId();
        assert localNodeId != null;

        Set<Index> indicesWithShards = new HashSet<>();
        RoutingNode localRoutingNode = event.state().getRoutingNodes().node(localNodeId);
        if (localRoutingNode != null) { // null e.g. if we are not a data node
            for (ShardRouting shard : localRoutingNode) {
                indicesWithShards.add(shard.index());
            }
        }

        Set<Index> currentRemovedIndices = new HashSet<>();
        for (Index index : currentIndices.keySet()) {
            if (indicesWithShards.contains(index) == false) {
                boolean notRemovedAlready = currentRemovedIndices.add(index);
                if (notRemovedAlready) {
                    logger.debug("{} removing index, no shards allocated", index);
                    indicesService.removeIndex(index, "removing index (no shards allocated)");
                }
            }
        }

        return currentRemovedIndices;
    }

    private Set<ShardId> applyRemovedShards(final ClusterChangedEvent event, Map<ShardId, ShardRouting> currentShards) {
        final RoutingTable routingTable = event.state().routingTable();
        final DiscoveryNodes nodes = event.state().nodes();
        final String localNodeId = event.state().nodes().getLocalNodeId();
        assert localNodeId != null;

        // remove shards based on routing nodes (no deletion of data)
        RoutingNode localRoutingNode = event.state().getRoutingNodes().node(localNodeId);
        Map<ShardId, ShardRouting> newShardAllocations = new HashMap<>();
        if (localRoutingNode != null) { // null e.g. if we are not a data node
            for (ShardRouting shard : localRoutingNode) {
                newShardAllocations.put(shard.shardId(), shard);
            }
        }

        Set<ShardId> removedShards = new HashSet<>();
        for (ShardRouting currentRoutingEntry : currentShards.values()) {
            ShardRouting newShardRouting = newShardAllocations.get(currentRoutingEntry.shardId());
            if (newShardRouting == null || newShardRouting.isSameAllocation(currentRoutingEntry) == false) {
                // we can just remove the shard, without cleaning it locally, since we will clean it
                // when all shards are allocated in the IndicesStore
                logger.debug("{} removing shard (not allocated)", currentRoutingEntry.shardId());
                removedShards.add(currentRoutingEntry.shardId());
                indicesService.removeShard(currentRoutingEntry.shardId(), "removing shard (not allocated)");
            } else {
                assert newShardRouting.isSameAllocation(currentRoutingEntry);
                // remove shards where recovery target has changed. This re-initializes shards later in applyNewOrUpdatedShards
                if (newShardRouting.isPeerRecovery()) {
                    RecoveryState recoveryState = indicesService.recoveryState(currentRoutingEntry.shardId());
                    if (recoveryState != null && recoveryState.getStage() != RecoveryState.Stage.DONE) { // shard is still recovering
                        if (recoveryState.getType() == RecoveryState.Type.PRIMARY_RELOCATION || recoveryState.getType() == RecoveryState.Type.REPLICA) {
                            final DiscoveryNode sourceNode = findSourceNodeForPeerRecovery(logger, routingTable, nodes, newShardRouting);
                            if (recoveryState.getSourceNode().equals(sourceNode) == false) {
                                logger.debug("[{}][{}] removing shard (recovery source changed), current [{}], global [{}])", newShardRouting.index(), newShardRouting.id(), currentRoutingEntry, newShardRouting);
                                // closing the shard will also cancel any ongoing recovery.
                                indicesService.removeShard(newShardRouting.shardId(), "removing shard (recovery source node changed)");
                                removedShards.add(newShardRouting.shardId());
                            }
                        }
                    }
                }
            }
        }

        return removedShards;
    }

    private void applyNewIndices(final ClusterChangedEvent event, Map<Index, IndexSettings> currentIndices) {
        // we only create indices for shards that are allocated
        RoutingNode localRoutingNode = event.state().getRoutingNodes().node(event.state().nodes().getLocalNodeId());
        if (localRoutingNode == null) {
            return;
        }
        Set<Index> failedIndices = new HashSet<>(); // indices where creation failed (to make sure we only try once for each index)
        Set<Index> createdIndices = new HashSet<>(); // indices where creation has happened
        for (ShardRouting shard : localRoutingNode) {
            if (failedShardsCache.containsKey(shard.shardId())) {
                // ignore
                continue;
            }
            Index index = shard.index();
            if (failedIndices.contains(index)) {
                // index creation for this index failed in an earlier iteration for another shard with same index
                sendFailShard(shard, "failed to create index (for shard with same index)", null);
            } else {
                if (createdIndices.contains(index) == false && currentIndices.containsKey(index) == false) {
                    final IndexMetaData indexMetaData = event.state().metaData().index(shard.index());
                    logger.debug("[{}] creating index", index);
                    createdIndices.add(index);
                    boolean indexCreated = false;
                    try {
                        indicesService.createIndex(nodeServicesProvider, indexMetaData, buildInIndexListener);
                        indexCreated = true;
                    } catch (Throwable e) {
                        sendFailShard(shard, "failed to create index", e);
                        failedIndices.add(index);
                    }
                    if (indexCreated) {
                        try {
                            updateIndexMapping(event, indexMetaData);
                        } catch (Throwable e) {
                            indicesService.removeIndex(index, "removing newly created index (mapping update failed)");
                            // if we failed the mappings anywhere, we need to fail the shards for this index, note, we safeguard
                            // by creating the processing the mappings on the master, or on the node the mapping was introduced on,
                            // so this failure typically means wrong node level configuration or something similar
                            sendFailShard(shard, "failed to update mapping for newly created index", e);
                            failedIndices.add(index);
                        }
                    }
                }
            }
        }
    }

    private void applyUpdateExistingIndices(ClusterChangedEvent event, Map<Index, IndexSettings> currentIndices,
                                            Set<Index> currentDeletedIndices, Set<Index> currentRemovedIndices,
                                            Map<ShardId, ShardRouting> currentShards) {
        if (!event.metaDataChanged()) {
            return;
        }
        for (IndexMetaData indexMetaData : event.state().metaData()) {
            Index index = indexMetaData.getIndex();
            if (event.indexMetaDataChanged(indexMetaData) && currentIndices.containsKey(index)
                && currentDeletedIndices.contains(index) == false
                && currentRemovedIndices.contains(index) == false) {
                indicesService.updateMetaData(indexMetaData);
                try {
                    updateIndexMapping(event, indexMetaData);
                } catch (Throwable t) {
                    indicesService.removeIndex(index, "removing index (mapping update failed)");
                    // if we failed the mappings anywhere, we need to fail the shards for this index, note, we safeguard
                    // by creating the processing the mappings on the master, or on the node the mapping was introduced on,
                    // so this failure typically means wrong node level configuration or something similar
                    for (Map.Entry<ShardId, ShardRouting> entry : currentShards.entrySet()) {
                        if (index.equals(entry.getKey().getIndex())) {
                            sendFailShard(entry.getValue(), "failed to update mappings for newly created index", t);
                        }
                    }
                }
            }
        }
    }

    private void updateIndexMapping(ClusterChangedEvent event, IndexMetaData indexMetaData) throws Throwable {
        if (indicesService.requiresIndexMappingRefresh(indexMetaData) && sendRefreshMapping) {
            nodeMappingRefreshAction.nodeMappingRefresh(event.state().nodes().getMasterNode(),
                new NodeMappingRefreshAction.NodeMappingRefreshRequest(indexMetaData.getIndex().getName(), indexMetaData.getIndexUUID(),
                    event.state().nodes().getLocalNodeId())
            );
        }
    }

    private void applyNewOrUpdatedShards(final ClusterChangedEvent event, Map<ShardId, ShardRouting> currentShards) {
        if (!indicesService.changesAllowed()) {
            return;
        }

        RoutingNode localRoutingNode = event.state().getRoutingNodes().node(event.state().nodes().getLocalNodeId());
        if (localRoutingNode == null) {
            return;
        }

        DiscoveryNodes nodes = event.state().nodes();
        RoutingTable routingTable = event.state().routingTable();

        for (final ShardRouting shardRouting : localRoutingNode) {
            if (failedShardsCache.containsKey(shardRouting.shardId())) {
                // don't create or update failed shards
                continue;
            }

            ShardRouting currentRoutingEntry = currentShards.get(shardRouting.shardId());
            if (currentRoutingEntry == null) {
                createShard(nodes, routingTable, shardRouting);
            } else {
                updateShard(event, nodes, shardRouting, currentRoutingEntry);
            }
        }
    }

    private void createShard(DiscoveryNodes nodes, RoutingTable routingTable, ShardRouting shardRouting) {
        assert shardRouting.initializing();

        if (shardRouting.initializing()) {
            DiscoveryNode sourceNode = null;
            if (shardRouting.isPeerRecovery()) {
                sourceNode = findSourceNodeForPeerRecovery(logger, routingTable, nodes, shardRouting);
                if (sourceNode == null) {
                    logger.trace("ignoring initializing shard {} - no source node can be found.", shardRouting.shardId());
                    return;
                }
            }

            try {
                logger.debug("{} creating shard", shardRouting.shardId());
                indicesService.createShard(shardRouting, failedShardHandler, nodes.getLocalNode(), sourceNode, recoveryTargetService,
                    new RecoveryListener(shardRouting), repositoriesService);
            } catch (IndexShardAlreadyExistsException e) {
                // ignore this, the method call can happen several times
            } catch (Throwable e) {
                failAndRemoveShard(shardRouting, true, "failed to create shard", e);
            }
        }
    }

    private void updateShard(ClusterChangedEvent event, DiscoveryNodes nodes, ShardRouting shardRouting, ShardRouting currentRoutingEntry) {
        assert currentRoutingEntry.isSameAllocation(shardRouting) :
            "local shard has a different allocation id but wasn't cleaning by applyRemovedShards. "
                + "cluster state: " + shardRouting + " local: " + currentRoutingEntry;

        IndexShardState state;
        try {
            state = indicesService.updateShardRouting(shardRouting, event.state().blocks().disableStatePersistence() == false);
        } catch (Throwable e) {
            failAndRemoveShard(shardRouting, true, "failed updating shard routing entry", e);
            return;
        }

        if (state != null && shardRouting.initializing() && (state == IndexShardState.STARTED || state == IndexShardState.POST_RECOVERY)) {
            // the master thinks we are initializing, but we are already started or on POST_RECOVERY and waiting
            // for master to confirm a shard started message (either master failover, or a cluster event before
            // we managed to tell the master we started), mark us as started
            if (logger.isTraceEnabled()) {
                logger.trace("{} master marked shard as initializing, but shard has state [{}], resending shard started to {}",
                    shardRouting.shardId(), state, nodes.getMasterNode());
            }
            if (nodes.getMasterNode() != null) {
                shardStateAction.shardStarted(shardRouting,
                    "master " + nodes.getMasterNode() + " marked shard as initializing, but shard state is [" + state + "], mark shard as started",
                    SHARD_STATE_ACTION_LISTENER);
            }
        }
    }

    /**
     * Finds the routing source node for peer recovery, return null if its not found. Note, this method expects the shard
     * routing to *require* peer recovery, use {@link ShardRouting#isPeerRecovery()} to
     * check if its needed or not.
     */
    private static DiscoveryNode findSourceNodeForPeerRecovery(ESLogger logger, RoutingTable routingTable, DiscoveryNodes nodes, ShardRouting shardRouting) {
        DiscoveryNode sourceNode = null;
        if (!shardRouting.primary()) {
            ShardRouting primary = routingTable.shardRoutingTable(shardRouting.shardId()).primaryShard();
            // only recover from started primary, if we can't find one, we will do it next round
            if (primary.active()) {
                sourceNode = nodes.get(primary.currentNodeId());
                if (sourceNode == null) {
                    logger.trace("can't find replica source node because primary shard {} is assigned to an unknown node.", primary);
                }
            } else {
                logger.trace("can't find replica source node because primary shard {} is not active.", primary);
            }
        } else if (shardRouting.relocatingNodeId() != null) {
            sourceNode = nodes.get(shardRouting.relocatingNodeId());
            if (sourceNode == null) {
                logger.trace("can't find relocation source node for shard {} because it is assigned to an unknown node [{}].", shardRouting.shardId(), shardRouting.relocatingNodeId());
            }
        } else {
            throw new IllegalStateException("trying to find source node for peer recovery when routing state means no peer recovery: " + shardRouting);
        }
        return sourceNode;
    }

    private class RecoveryListener implements RecoveryTargetService.RecoveryListener {

        private final ShardRouting shardRouting;

        private RecoveryListener(ShardRouting shardRouting) {
            this.shardRouting = shardRouting;
        }

        @Override
        public void onRecoveryDone(RecoveryState state) {
            if (state.getType() == RecoveryState.Type.SNAPSHOT) {
                restoreService.indexShardRestoreCompleted(state.getRestoreSource().snapshotId(), shardRouting.shardId());
            }
            shardStateAction.shardStarted(shardRouting, message(state), SHARD_STATE_ACTION_LISTENER);
        }

        private String message(RecoveryState state) {
            switch (state.getType()) {
                case SNAPSHOT: return "after recovery from repository";
                case STORE: return "after recovery from store";
                case PRIMARY_RELOCATION: return "after recovery (primary relocation) from node [" + state.getSourceNode() + "]";
                case REPLICA: return "after recovery (replica) from node [" + state.getSourceNode() + "]";
                default: throw new IllegalArgumentException(state.getType().name());
            }
        }

        @Override
        public void onRecoveryFailure(RecoveryState state, RecoveryFailedException e, boolean sendShardFailure) {
            if (state.getType() == RecoveryState.Type.SNAPSHOT) {
                try {
                    if (Lucene.isCorruptionException(e.getCause())) {
                        restoreService.failRestore(state.getRestoreSource().snapshotId(), shardRouting.shardId());
                    }
                } catch (Throwable inner) {
                    e.addSuppressed(inner);
                } finally {
                    handleRecoveryFailure(shardRouting, sendShardFailure, e);
                }
            } else {
                handleRecoveryFailure(shardRouting, sendShardFailure, e);
            }
        }
    }

    private void handleRecoveryFailure(ShardRouting shardRouting, boolean sendShardFailure, Throwable failure) {
        synchronized (mutex) {
            failAndRemoveShard(shardRouting, sendShardFailure, "failed recovery", failure);
        }
    }

    private void failAndRemoveShard(ShardRouting shardRouting, boolean sendShardFailure, String message, @Nullable Throwable failure) {
        try {
            indicesService.removeShard(shardRouting.shardId(), message);
        } catch (ShardNotFoundException e) {
            // the node got closed on us, ignore it
        } catch (Throwable e1) {
            logger.warn("[{}][{}] failed to remove shard after failure ([{}])", e1, shardRouting.getIndexName(), shardRouting.getId(), message);
        }
        if (sendShardFailure) {
            sendFailShard(shardRouting, message, failure);
        }
    }

    private void sendFailShard(ShardRouting shardRouting, String message, @Nullable Throwable failure) {
        try {
            logger.warn("[{}] marking and sending shard failed due to [{}]", failure, shardRouting.shardId(), message);
            failedShardsCache.put(shardRouting.shardId(), shardRouting);
            shardStateAction.shardFailed(shardRouting, shardRouting, message, failure, SHARD_STATE_ACTION_LISTENER);
        } catch (Throwable e1) {
            logger.warn("[{}][{}] failed to mark shard as failed (because of [{}])", e1, shardRouting.getIndexName(), shardRouting.getId(), message);
        }
    }

    private class FailedShardHandler implements Callback<IndexShard.ShardFailure> {
        @Override
        public void handle(final IndexShard.ShardFailure shardFailure) {
            final ShardRouting shardRouting = shardFailure.routing;
            threadPool.generic().execute(() -> {
                synchronized (mutex) {
                    failAndRemoveShard(shardRouting, true, "shard failure, reason [" + shardFailure.reason + "]", shardFailure.cause);
                }
            });
        }
    }

    public static class IndicesServiceProxy {
        private final ESLogger logger;
        private final IndicesService indicesService;

        public IndicesServiceProxy(ESLogger logger, IndicesService indicesService) {
            this.logger = logger;
            this.indicesService = indicesService;
        }

        public Map<Index, IndexSettings> indices() {
            Map<Index, IndexSettings> indices = new HashMap<>();
            for (IndexService indexService : indicesService) {
                indices.put(indexService.index(), indexService.getIndexSettings());
            }
            return indices;
        }

        public Map<ShardId, ShardRouting> shards() {
            Map<ShardId, ShardRouting> shards = new HashMap<>();
            for (IndexService indexService : indicesService) {
                for (IndexShard indexShard : indexService) {
                    ShardRouting shardRouting = indexShard.routingEntry();
                    shards.put(shardRouting.shardId(), shardRouting);
                }
            }
            return shards;
        }

        private void removeIndex(Index index, String reason) {
            try {
                indicesService.removeIndex(index, reason);
            } catch (Throwable e) {
                logger.warn("failed to clean index ({})", e, reason);
            }
        }

        private void deleteIndex(Index index, String reason) {
            try {
                indicesService.deleteIndex(index, reason);
            } catch (Throwable e) {
                logger.warn("failed to delete index ({})", e, reason);
            }
        }

        private void removeShard(ShardId shardId, String message) {
            IndexService indexService = indicesService.indexService(shardId.getIndex());
            if (indexService != null) {
                indexService.removeShard(shardId.id(), message);
            }
        }

        private RecoveryState recoveryState(ShardId shardId) {
            IndexService indexService = indicesService.indexService(shardId.getIndex());
            if (indexService != null) {
                IndexShard indexShard = indexService.getShardOrNull(shardId.id());
                if (indexShard != null) {
                    return indexShard.recoveryState();
                }
            }
            return null;
        }

        public boolean changesAllowed() {
            return indicesService.changesAllowed();
        }

        public void deleteUnassignedIndex(String reason, IndexMetaData metaData, ClusterState clusterState) {
            indicesService.deleteUnassignedIndex(reason, metaData, clusterState);
        }

        public IndexMetaData verifyIndexIsDeleted(Index index, ClusterState state) {
            return indicesService.verifyIndexIsDeleted(index, state);
        }

        public void createIndex(NodeServicesProvider nodeServicesProvider, IndexMetaData indexMetaData, List<IndexEventListener> buildInIndexListener) throws IOException {
            indicesService.createIndex(nodeServicesProvider, indexMetaData, buildInIndexListener);
        }

        private void updateMetaData(IndexMetaData indexMetaData) {
            IndexService indexService = indicesService.indexService(indexMetaData.getIndex());
            if (indexService == null) {
                return;
            }
            indexService.updateMetaData(indexMetaData);
        }

        public boolean requiresIndexMappingRefresh(IndexMetaData indexMetaData) throws IOException {
            IndexService indexService = indicesService.indexService(indexMetaData.getIndex());
            if (indexService == null) {
                return false;
            }
            return indexService.requiresIndexMappingRefresh(indexMetaData);
        }

        public IndexShardState updateShardRouting(ShardRouting shardRouting, boolean persistState) throws IOException {
            final IndexService indexService = indicesService.indexService(shardRouting.index());
            if (indexService == null) {
                // creation failed for some reasons
                return null;
            }
            IndexShard indexShard = indexService.getShardOrNull(shardRouting.id());
            if (indexShard == null) {
                return null;
            }

            indexShard.updateRoutingEntry(shardRouting, persistState);

            return indexShard.state();
        }

        public void createShard(ShardRouting shardRouting, FailedShardHandler failedShardHandler, DiscoveryNode localNode,
                                DiscoveryNode sourceNode, RecoveryTargetService recoveryTargetService, RecoveryListener recoveryListener,
                                RepositoriesService repositoriesService) throws IOException {
            final IndexService indexService = indicesService.indexService(shardRouting.index());
            if (indexService == null) {
                // creation failed for some reasons
                return;
            }

            IndexShard indexShard = indexService.createShard(shardRouting);
            indexShard.addShardFailureCallback(failedShardHandler);
            indexShard.startRecovery(shardRouting, localNode, sourceNode, recoveryTargetService, recoveryListener, repositoriesService);
        }
    }
}
