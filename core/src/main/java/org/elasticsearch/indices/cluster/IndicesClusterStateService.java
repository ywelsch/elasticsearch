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
import org.elasticsearch.cluster.action.index.NodeMappingRefreshAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
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
import org.elasticsearch.index.IndexComponent;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexShardAlreadyExistsException;
import org.elasticsearch.index.NodeServicesProvider;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardRelocatedException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.IndexAlreadyExistsException;
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

    final BaseIndicesService<? extends Shard, ? extends BaseIndexService<? extends Shard>> indicesService;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final RecoveryTargetService recoveryTargetService;
    private final ShardStateAction shardStateAction;
    private final NodeMappingRefreshAction nodeMappingRefreshAction;
    private final NodeServicesProvider nodeServicesProvider;

    private static final ShardStateAction.Listener SHARD_STATE_ACTION_LISTENER = new ShardStateAction.Listener() {
    };

    // a list of shards that failed during recovery
    // we keep track of these shards in order to prevent repeated recovery of these shards on each cluster state update
    final ConcurrentMap<ShardId, ShardRouting> failedShardsCache = ConcurrentCollections.newConcurrentMap();
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
                                      NodeMappingRefreshAction nodeMappingRefreshAction,
                                      RepositoriesService repositoriesService, RestoreService restoreService,
                                      SearchService searchService, SyncedFlushService syncedFlushService,
                                      RecoverySource recoverySource, NodeServicesProvider nodeServicesProvider) {
        this(settings, (BaseIndicesService<? extends Shard, ? extends BaseIndexService<? extends Shard>>) indicesService,
            clusterService, threadPool, recoveryTargetService, shardStateAction,
            nodeMappingRefreshAction, repositoriesService, restoreService, searchService, syncedFlushService, recoverySource,
            nodeServicesProvider);
    }

    // for tests
    IndicesClusterStateService(Settings settings,
                               BaseIndicesService<? extends Shard, ? extends BaseIndexService<? extends Shard>> indicesService,
                               ClusterService clusterService,
                               ThreadPool threadPool, RecoveryTargetService recoveryTargetService,
                               ShardStateAction shardStateAction,
                               NodeMappingRefreshAction nodeMappingRefreshAction,
                               RepositoriesService repositoriesService, RestoreService restoreService,
                               SearchService searchService, SyncedFlushService syncedFlushService,
                               RecoverySource recoverySource, NodeServicesProvider nodeServicesProvider) {
        super(settings);
        this.buildInIndexListener = Arrays.asList(recoverySource, recoveryTargetService, searchService, syncedFlushService);
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.recoveryTargetService = recoveryTargetService;
        this.shardStateAction = shardStateAction;
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
            // we need to clean the shards and indices we have on this node, since we
            // are going to recover them again once state persistence is disabled (no master / not recovered)
            // TODO: this feels a bit hacky here, a block disables state persistence, and then we clean the allocated shards, maybe another flag in blocks?
            if (event.state().blocks().disableStatePersistence()) {
                for (BaseIndexService<? extends Shard> indexService : indicesService) {
                    indicesService.removeIndex(indexService.index(), "cleaning index (disabled block persistence)"); // also cleans shards
                }
                return;
            }

            updateFailedShardsCache(event);

            deleteIndices(event); // also deletes shards of deleted indices

            removeIndices(event); // also removes shards of removed indices

            removeShards(event);

            updateIndices(event); // can also fail shards, but these are then guaranteed to be in failedShardsCache

            createIndices(event);

            createOrUpdateShards(event);
        }
    }

    /**
     * Removes shard entries from the failed shards cache that are no longer allocated to this node by the master.
     * Sends shard failures for shards that are marked as actively allocated to this node but don't actually exist on the node.
     * Resends shard failures for shards that are still marked as allocated to this node but previously failed.
     *
     * @param event cluster change event
     */
    private void updateFailedShardsCache(final ClusterChangedEvent event) {
        RoutingNode localRoutingNode = event.state().getRoutingNodes().node(event.state().nodes().getLocalNodeId());
        if (localRoutingNode == null) {
            failedShardsCache.clear();
            return;
        }

        // remove items from cache which are not in our routing table anymore
        for (Iterator<Map.Entry<ShardId, ShardRouting>> iterator = failedShardsCache.entrySet().iterator(); iterator.hasNext(); ) {
            ShardRouting failedShardRouting = iterator.next().getValue();
            ShardRouting matchedShardRouting = localRoutingNode.getByShardId(failedShardRouting.shardId());
            if (matchedShardRouting == null || matchedShardRouting.isSameAllocation(failedShardRouting) == false) {
                iterator.remove();
            }
        }

        DiscoveryNode masterNode = event.state().nodes().getMasterNode();
        for (ShardRouting shardRouting : localRoutingNode) {
            if (failedShardsCache.containsKey(shardRouting.shardId())) {
                if (masterNode != null) { // TODO: can we remove this? Isn't resending shard failures the responsibility of shardStateAction?
                    String message = "master " + masterNode + " has not removed shard, but shard has previous failed. resending shard failure";
                    logger.trace("[{}] re-sending failed shard [{}], reason [{}]", shardRouting.shardId(), shardRouting, message);
                    shardStateAction.shardFailed(shardRouting, shardRouting, message, null, SHARD_STATE_ACTION_LISTENER);
                }
            } else {
                if (shardRouting.initializing() == false && indicesService.getShardOrNull(shardRouting.shardId()) == null) {
                    // the master thinks we are active, but we don't have this shard at all, mark it as failed
                    sendFailShard(shardRouting, "master [" + masterNode + "] marked shard as active, but shard has not been created, mark shard as failed", null);
                }
            }
        }
    }

    /**
     * Deletes indices (with shard data).
     *
     * @param event cluster change event
     */
    private void deleteIndices(final ClusterChangedEvent event) {
        final ClusterState previousState = event.previousState();
        final String localNodeId = event.state().nodes().getLocalNodeId();
        assert localNodeId != null;

        for (Index index : event.indicesDeleted()) {
            if (logger.isDebugEnabled()) {
                logger.debug("[{}] cleaning index, no longer part of the metadata", index);
            }

            BaseIndexService<? extends Shard> indexService = indicesService.indexService(index);
            final IndexSettings indexSettings;
            if (indexService != null) {
                indexSettings = indexService.getIndexSettings();
                try {
                    indicesService.deleteIndex(index, "index no longer part of the metadata");
                } catch (Throwable e) {
                    logger.warn("failed to delete index ({})", e, "index no longer part of the metadata");
                }
            } else if (previousState.metaData().hasIndex(index.getName())) {
                // The deleted index was part of the previous cluster state, but not loaded on the local node
                final IndexMetaData metaData = previousState.metaData().index(index);
                indicesService.deleteUnassignedIndex("deleted index was not assigned to local node", metaData, event.state());
            } else {
                // The previous cluster state's metadata also does not contain the index,
                // which is what happens on node startup when an index was deleted while the
                // node was not part of the cluster.  In this case, try reading the index
                // metadata from disk.  If its not there, there is nothing to delete.
                // First, though, verify the precondition for applying this case by
                // asserting that the previous cluster state is not initialized/recovered.
                assert previousState.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK);
                indicesService.verifyIndexIsDeleted(index, event.state());
            }
        }

        // delete local indices that do neither exist in previous cluster state nor part of tombstones
        for (BaseIndexService<? extends Shard> indexService : indicesService) {
            Index index = indexService.index();
            IndexMetaData indexMetaData = event.state().metaData().index(index);
            if (indexMetaData == null) {
                assert false : "index" + index + " exists locally, doesn't have a metadata but is not part"
                    + " of the delete index list. \nprevious state: " + event.previousState().prettyPrint()
                    + "\n current state:\n" + event.state().prettyPrint();
                logger.warn("[{}] isn't part of metadata but is part of in memory structures. removing", index);
                try {
                    indicesService.deleteIndex(index, "isn't part of metadata (explicit check)");
                } catch (Throwable e) {
                    logger.warn("failed to delete index ({})", e, "isn't part of metadata (explicit check)");
                }
            }
        }
    }

    /**
     * Removes indices that have no shards allocated to this node. This does not delete the shard data.
     *
     * @param event cluster change event
     */
    private void removeIndices(final ClusterChangedEvent event) {
        final String localNodeId = event.state().nodes().getLocalNodeId();
        assert localNodeId != null;

        Set<Index> indicesWithShards = new HashSet<>();
        RoutingNode localRoutingNode = event.state().getRoutingNodes().node(localNodeId);
        if (localRoutingNode != null) { // null e.g. if we are not a data node
            for (ShardRouting shardRouting : localRoutingNode) {
                indicesWithShards.add(shardRouting.index());
            }
        }

        for (BaseIndexService<? extends Shard> indexService : indicesService) {
            Index index = indexService.index();
            if (indicesWithShards.contains(index) == false) {
                logger.debug("{} removing index, no shards allocated", index);
                try {
                    indicesService.removeIndex(index, "removing index (no shards allocated)");
                } catch (Throwable e) {
                    logger.warn("{} failed to clean index ({})", e, index, "no shards allocated");
                }
            }
        }
    }

    /**
     * Removes shards that are currently loaded by indicesService but have disappeared from the routing table of the current node.
     * Also removes shards where the recovery source node has changed.
     * This method does not delete the shard data.
     *
     * @param event cluster state change event
     */
    private void removeShards(final ClusterChangedEvent event) {
        final RoutingTable routingTable = event.state().routingTable();
        final DiscoveryNodes nodes = event.state().nodes();
        final String localNodeId = event.state().nodes().getLocalNodeId();
        assert localNodeId != null;

        // remove shards based on routing nodes (no deletion of data)
        RoutingNode localRoutingNode = event.state().getRoutingNodes().node(localNodeId);
        for (BaseIndexService<? extends Shard> indexService : indicesService) {
            for (Shard shard : indexService) {
                ShardRouting currentRoutingEntry = shard.routingEntry();
                ShardId shardId = currentRoutingEntry.shardId();
                ShardRouting newShardRouting = localRoutingNode == null ? null : localRoutingNode.getByShardId(shardId);
                if (newShardRouting == null || newShardRouting.isSameAllocation(currentRoutingEntry) == false) {
                    // we can just remove the shard without cleaning it locally, since we will clean it in IndicesStore
                    // once all shards are allocated
                    logger.debug("{} removing shard (not allocated)", shardId);
                    indexService.removeShard(shardId.id(), "removing shard (not allocated)");
                } else {
                    assert newShardRouting.isSameAllocation(currentRoutingEntry);
                    // remove shards where recovery source has changed. This re-initializes shards later in createOrUpdateShards
                    if (newShardRouting.isPeerRecovery()) {
                        RecoveryState recoveryState = shard.recoveryState();
                        final DiscoveryNode sourceNode = findSourceNodeForPeerRecovery(logger, routingTable, nodes, newShardRouting);
                        if (recoveryState.getSourceNode().equals(sourceNode) == false) {
                            if (recoveryTargetService.cancelRecoveriesForShard(shardId, "recovery source node changed")) {
                                // getting here means that the shard was still recovering
                                logger.debug("{} removing shard (recovery source changed), current [{}], global [{}])",
                                    shardId, currentRoutingEntry, newShardRouting);
                                indexService.removeShard(shardId.id(), "removing shard (recovery source node changed)");
                            }
                        }
                    }
                }
            }
        }
    }

    private void createIndices(final ClusterChangedEvent event) {
        // we only create indices for shards that are allocated
        RoutingNode localRoutingNode = event.state().getRoutingNodes().node(event.state().nodes().getLocalNodeId());
        if (localRoutingNode == null) {
            return;
        }
        Set<Index> failedIndices = new HashSet<>(); // indices where creation failed (to make sure we only try creation once for each index)
        for (ShardRouting shardRouting : localRoutingNode) {
            if (failedShardsCache.containsKey(shardRouting.shardId())) {
                // ignore
                continue;
            }
            Index index = shardRouting.index();
            if (failedIndices.contains(index)) {
                // index creation for this index failed in an earlier iteration for another shard with same index
                sendFailShard(shardRouting, "failed to create index (for shard with same index)", null);
            } else if (indicesService.indexService(index) == null) {
                final IndexMetaData indexMetaData = event.state().metaData().index(shardRouting.index());
                logger.debug("[{}] creating index", index);
                BaseIndexService<? extends Shard> indexService = null;
                try {
                    indexService = indicesService.createIndex(nodeServicesProvider, indexMetaData, buildInIndexListener);
                } catch (Throwable e) {
                    sendFailShard(shardRouting, "failed to create index", e);
                    failedIndices.add(index);
                }
                if (indexService != null) {
                    updateIndexMapping(event, indexService, indexMetaData, failedIndices);
                }
            }
        }
    }

    private void updateIndices(ClusterChangedEvent event) {
        if (!event.metaDataChanged()) {
            return;
        }
        for (BaseIndexService<? extends Shard> indexService : indicesService) {
            Index index = indexService.index();
            IndexMetaData indexMetaData = event.state().metaData().index(index);
            assert indexMetaData != null : "index should have been deleted by applyDeleteIndices";
            if (event.indexMetaDataChanged(indexMetaData)) {
                indexService.updateMetaData(indexMetaData);
                updateIndexMapping(event, indexService, indexMetaData, null);
            }
        }
    }

    private void updateIndexMapping(ClusterChangedEvent event, BaseIndexService<? extends Shard> indexService, IndexMetaData indexMetaData,
                                    @Nullable Set<Index> failedIndices) {
        try {
            if (indexService.requiresIndexMappingRefresh(indexMetaData) && sendRefreshMapping) {
                nodeMappingRefreshAction.nodeMappingRefresh(event.state().nodes().getMasterNode(),
                    new NodeMappingRefreshAction.NodeMappingRefreshRequest(indexMetaData.getIndex().getName(), indexMetaData.getIndexUUID(),
                        event.state().nodes().getLocalNodeId())
                );
            }
        } catch (Throwable t) {
            // if we failed the mappings anywhere, we need to fail the shards for this index, note, we safeguard
            // by creating the processing the mappings on the master, or on the node the mapping was introduced on,
            // so this failure typically means wrong node level configuration or something similar
            if (failedIndices != null) {
                failedIndices.add(indexService.index());
            }
            Set<ShardRouting> shardRoutings = new HashSet<>();
            for (Shard shard : indexService) {
                shardRoutings.add(shard.routingEntry());
            }
            indicesService.removeIndex(indexService.index(), "removing index (mapping update failed)");
            for (ShardRouting shardRouting : shardRoutings) {
                sendFailShard(shardRouting, "failed to update mapping for index", t);
            }
        }
    }

    private void createOrUpdateShards(final ClusterChangedEvent event) {
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
            ShardId shardId = shardRouting.shardId();
            if (failedShardsCache.containsKey(shardId)) {
                // don't create or update failed shards
                continue;
            }

            BaseIndexService<? extends Shard> indexService = indicesService.indexService(shardId.getIndex());
            assert indexService != null;
            if (indexService != null) {
                Shard shard = indexService.getShardOrNull(shardId.id());
                if (shard == null) {
                    createShard(nodes, routingTable, shardRouting, indexService);
                } else {
                    updateShard(event, nodes, shardRouting, shard);
                }
            }
        }
    }

    private void createShard(DiscoveryNodes nodes, RoutingTable routingTable, ShardRouting shardRouting,
                             BaseIndexService<? extends Shard> indexService) {
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
                RecoveryState recoveryState = recoveryState(nodes.getLocalNode(), sourceNode, shardRouting,
                    indexService.getIndexSettings().getIndexMetaData());
                indicesService.createShard(shardRouting, recoveryState, recoveryTargetService, new RecoveryListener(shardRouting),
                    repositoriesService, nodeServicesProvider, failedShardHandler);
            } catch (IndexShardAlreadyExistsException e) {
                // ignore this, the method call can happen several times
                assert false;
            } catch (Throwable e) {
                failAndRemoveShard(shardRouting, true, "failed to create shard", e);
            }
        }
    }

    private void updateShard(ClusterChangedEvent event, DiscoveryNodes nodes, ShardRouting shardRouting, Shard shard) {
        final ShardRouting currentRoutingEntry = shard.routingEntry();
        assert currentRoutingEntry.isSameAllocation(shardRouting) :
            "local shard has a different allocation id but wasn't cleaning by removeShards. "
                + "cluster state: " + shardRouting + " local: " + currentRoutingEntry;

        try {
            shard.updateRoutingEntry(shardRouting, event.state().blocks().disableStatePersistence() == false);
        } catch (Throwable e) {
            failAndRemoveShard(shardRouting, true, "failed updating shard routing entry", e);
            return;
        }

        final IndexShardState state = shard.state();
        if (shardRouting.initializing() && (state == IndexShardState.STARTED || state == IndexShardState.POST_RECOVERY)) {
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
                restoreService.indexShardRestoreCompleted(state.getRestoreSource().snapshot(), shardRouting.shardId());
            }
            shardStateAction.shardStarted(shardRouting, message(state), SHARD_STATE_ACTION_LISTENER);
        }

        private String message(RecoveryState state) {
            switch (state.getType()) {
                case SNAPSHOT: return "after recovery from repository";
                case STORE: return "after recovery from store";
                case PRIMARY_RELOCATION: return "after recovery (primary relocation) from node [" + state.getSourceNode() + "]";
                case REPLICA: return "after recovery (replica) from node [" + state.getSourceNode() + "]";
                case LOCAL_SHARDS: return "after recovery from local shards";
                default: throw new IllegalArgumentException("Unknown recovery type: " + state.getType().name());
            }
        }

        @Override
        public void onRecoveryFailure(RecoveryState state, RecoveryFailedException e, boolean sendShardFailure) {
            if (state.getType() == RecoveryState.Type.SNAPSHOT) {
                try {
                    if (Lucene.isCorruptionException(e.getCause())) {
                        restoreService.failRestore(state.getRestoreSource().snapshot(), shardRouting.shardId());
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

    private RecoveryState recoveryState(DiscoveryNode localNode, DiscoveryNode sourceNode, ShardRouting shardRouting,
                                              IndexMetaData indexMetaData) {
        assert shardRouting.initializing();
        if (shardRouting.isPeerRecovery()) {
            assert sourceNode != null : "peer recovery started but sourceNode is null";
            RecoveryState.Type type = shardRouting.primary() ? RecoveryState.Type.PRIMARY_RELOCATION : RecoveryState.Type.REPLICA;
            return new RecoveryState(shardRouting.shardId(), shardRouting.primary(), type, sourceNode, localNode);
        } else if (shardRouting.restoreSource() == null) {
            // recover from filesystem store
            Index mergeSourceIndex = indexMetaData.getMergeSourceIndex();
            final boolean recoverFromLocalShards = mergeSourceIndex != null && shardRouting.allocatedPostIndexCreate(indexMetaData) == false
                && shardRouting.primary();
            return new RecoveryState(shardRouting.shardId(), shardRouting.primary(),
                recoverFromLocalShards ? RecoveryState.Type.LOCAL_SHARDS : RecoveryState.Type.STORE, localNode, localNode);
        } else {
            // recover from a snapshot
            return new RecoveryState(shardRouting.shardId(), shardRouting.primary(),
                RecoveryState.Type.SNAPSHOT, shardRouting.restoreSource(), localNode);
        }
    }

    private void handleRecoveryFailure(ShardRouting shardRouting, boolean sendShardFailure, Throwable failure) {
        synchronized (mutex) {
            failAndRemoveShard(shardRouting, sendShardFailure, "failed recovery", failure);
        }
    }

    private void failAndRemoveShard(ShardRouting shardRouting, boolean sendShardFailure, String message, @Nullable Throwable failure) {
        try {
            BaseIndexService<? extends Shard> indexService = indicesService.indexService(shardRouting.shardId().getIndex());
            if (indexService != null) {
                indexService.removeShard(shardRouting.shardId().id(), message);
            }
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

    public interface Shard {

        /**
         * Returns the shard id of this shard.
         */
        ShardId shardId();

        /**
         * Returns the latest cluster routing entry received with this shard.
         */
        ShardRouting routingEntry();

        /**
         * Returns the latest internal shard state.
         */
        IndexShardState state();

        /**
         * Returns the recovery state associated with this shard.
         */
        RecoveryState recoveryState();

        /**
         * Updates the shards routing entry. This mutate the shards internal state depending
         * on the changes that get introduced by the new routing value. This method will persist shard level metadata
         * unless explicitly disabled.
         *
         * @throws IndexShardRelocatedException if shard is marked as relocated and relocation aborted
         * @throws IOException                  if shard state could not be persisted
         */
        void updateRoutingEntry(ShardRouting shardRouting, boolean persistState) throws IOException;
    }

    public interface BaseIndexService<T extends Shard> extends Iterable<T>, IndexComponent {

        /**
         * Returns the index settings of this index.
         */
        IndexSettings getIndexSettings();

        /**
         * Updates the meta data of this index. Changes become visible through {@link #getIndexSettings()}
         */
        void updateMetaData(IndexMetaData indexMetaData);

        /**
         * Checks if index requires refresh from master.
         */
        boolean requiresIndexMappingRefresh(IndexMetaData indexMetaData) throws IOException;

        /**
         * Returns shard with given id.
         */
        @Nullable T getShardOrNull(int shardId);

        /**
         * Removes shard with given id.
         */
        void removeShard(int shardId, String message);
    }

    public interface BaseIndicesService<T extends Shard, U extends BaseIndexService<T>> extends Iterable<U> {

        /**
         * Returns true if changes are allowed to be made to list of indices and shards.
         */
        boolean changesAllowed();

        /**
         * Creates a new {@link IndexService} for the given metadata.
         * @param indexMetaData the index metadata to create the index for
         * @param builtInIndexListener a list of built-in lifecycle {@link IndexEventListener} that should should be used along side with
         *                             the per-index listeners
         * @throws IndexAlreadyExistsException if the index already exists.
         */
        U createIndex(NodeServicesProvider nodeServicesProvider, IndexMetaData indexMetaData,
                         List<IndexEventListener> builtInIndexListener) throws IOException;

        /**
         * Verify that the contents on disk for the given index is deleted; if not, delete the contents.
         * This method assumes that an index is already deleted in the cluster state and/or explicitly
         * through index tombstones.
         * @param index {@code Index} to make sure its deleted from disk
         * @param clusterState {@code ClusterState} to ensure the index is not part of it
         * @return IndexMetaData for the index loaded from disk
         */
        IndexMetaData verifyIndexIsDeleted(Index index, ClusterState clusterState);

        /**
         * Deletes the given index. Persistent parts of the index
         * like the shards files, state and transaction logs are removed once all resources are released.
         *
         * Equivalent to {@link #removeIndex(Index, String)} but fires
         * different lifecycle events to ensure pending resources of this index are immediately removed.
         * @param index the index to delete
         * @param reason the high level reason causing this delete
         */
        void deleteIndex(Index index, String reason) throws IOException;

        /**
         * Deletes an index that is not assigned to this node. This method cleans up all disk folders relating to the index
         * but does not deal with in-memory structures. For those call {@link #deleteIndex(Index, String)}
         */
        void deleteUnassignedIndex(String reason, IndexMetaData metaData, ClusterState clusterState);

        /**
         * Removes the given index from this service and releases all associated resources. Persistent parts of the index
         * like the shards files, state and transaction logs are kept around in the case of a disaster recovery.
         * @param index the index to remove
         * @param reason  the high level reason causing this removal
         */
        void removeIndex(Index index, String reason);

        /**
         * Returns an IndexService for the specified index if exists otherwise returns <code>null</code>.
         */
        @Nullable U indexService(Index index);

        /**
         * Creates shard for the specified shard routing and starts recovery,
         */
        T createShard(ShardRouting shardRouting, RecoveryState recoveryState, RecoveryTargetService recoveryTargetService,
                      RecoveryTargetService.RecoveryListener recoveryListener, RepositoriesService repositoriesService,
                      NodeServicesProvider nodeServicesProvider, Callback<IndexShard.ShardFailure> onShardFailure) throws IOException;

        /**
         * Returns shard for the specified id if it exists otherwise returns <code>null</code>.
         */
        default T getShardOrNull(ShardId shardId) {
            U indexRef = indexService(shardId.getIndex());
            if (indexRef != null) {
                return indexRef.getShardOrNull(shardId.id());
            }
            return null;
        }
    }
}
