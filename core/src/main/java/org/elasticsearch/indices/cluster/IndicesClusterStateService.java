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

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.action.index.NodeIndexDeletedAction;
import org.elasticsearch.cluster.action.index.NodeMappingRefreshAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
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
import org.elasticsearch.common.compress.CompressedXContent;
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
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
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

    private final IndicesService indicesService;
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
        this.indicesService = indicesService;
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
            // we need to clean the shards and indices we have on this node, since we
            // are going to recover them again once state persistence is disabled (no master / not recovered)
            // TODO: this feels a bit hacky here, a block disables state persistence, and then we clean the allocated shards, maybe another flag in blocks?
            if (event.state().blocks().disableStatePersistence()) {
                for (IndexService indexService : indicesService) {
                    Index index = indexService.index();
                    for (Integer shardId : indexService.shardIds()) {
                        logger.debug("{}[{}] removing shard (disabled block persistence)", index, shardId);
                        try {
                            indexService.removeShard(shardId, "removing shard (disabled block persistence)");
                        } catch (Throwable e) {
                            logger.warn("{} failed to remove shard (disabled block persistence)", e, index);
                        }
                    }
                    removeIndex(index, "cleaning index (disabled block persistence)");
                }
                return;
            }

            updateFailedShardsCache(event);
            applyDeletedIndicesAndShards(event);
            applyUpdateExistingIndices(event); // can also fail shards, but these are then guaranteed to be in failedShards
            applyNewIndices(event);
            applyNewOrUpdatedShards(event);
        }
    }

    private void updateFailedShardsCache(final ClusterChangedEvent event) {
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
                IndexService indexService = indicesService.indexService(shard.index());
                if (shard.initializing() == false && (indexService == null || indexService.hasShard(shard.id()) == false)) {
                    // the master thinks we are active, but we don't have this shard at all, mark it as failed
                    sendFailShard(shard, "master [" + nodes.getMasterNode() + "] marked shard as active, but shard has not been created, mark shard as failed", null);
                    // in case shard is initializing, we expect it to be a shard that is to be allocated to this node
                }
            }
        }
    }

    private void applyDeletedIndicesAndShards(final ClusterChangedEvent event) {
        final ClusterState previousState = event.previousState();
        final DiscoveryNodes nodes = event.state().nodes();
        final RoutingTable routingTable = event.state().routingTable();
        final String localNodeId = event.state().nodes().getLocalNodeId();
        assert localNodeId != null;

        for (Index index : event.indicesDeleted()) {
            if (logger.isDebugEnabled()) {
                logger.debug("[{}] cleaning index, no longer part of the metadata", index);
            }
            final IndexService idxService = indicesService.indexService(index);
            final IndexSettings indexSettings;
            if (idxService != null) {
                indexSettings = idxService.getIndexSettings();
                deleteIndex(index, "index no longer part of the metadata");
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

        for (IndexService indexService : indicesService) {
            IndexMetaData indexMetaData = event.state().metaData().index(indexService.index());
            if (indexMetaData == null) {
                assert false : "index" + indexService.index() + " exists locally, doesn't have a metadata but is not part "
                    + " of the delete index list. \nprevious state: " + event.previousState().prettyPrint()
                    + "\n current state:\n" + event.state().prettyPrint();
                logger.warn("[{}] isn't part of metadata but is part of in memory structures. removing",
                    indexService.index());
                deleteIndex(indexService.index(), "isn't part of metadata (explicit check)");
            }
        }

        // remove shards and indices based on routing nodes (no deletion of data)
        RoutingNode localRoutingNode = event.state().getRoutingNodes().node(localNodeId);
        // remove shards / index based on info in routing nodes
        if (localRoutingNode != null) { // null e.g. if we are not a data node
            Map<String, ShardRouting> newShardAllocationIds = new HashMap<>();
            for (IndexService indexService : indicesService) {
                newShardAllocationIds.clear(); // reuse set across iterations
                Index index = indexService.index();
                // now, go over and collect allocation ids of all shards with the current index that should be allocated to this node
                for (ShardRouting shard : localRoutingNode) {
                    if (shard.index().equals(index)) {
                        // use the allocation id and not object so we won't be influence by relocation targets
                        newShardAllocationIds.put(shard.allocationId().getId(), shard);
                    }
                }
                for (IndexShard existingShard : indexService) {
                    ShardRouting currentRoutingEntry = existingShard.routingEntry();
                    ShardRouting shardRouting = newShardAllocationIds.get(currentRoutingEntry.allocationId().getId());
                    // remove shards that do not match allocation ids
                    if (shardRouting == null) {
                        // we can just remove the shard, without cleaning it locally, since we will clean it
                        // when all shards are allocated in the IndicesStore
                        if (logger.isDebugEnabled()) {
                            logger.debug("{} removing shard (not allocated)", existingShard.shardId());
                        }
                        indexService.removeShard(existingShard.shardId().id(), "removing shard (not allocated)");
                    } else {
                        assert currentRoutingEntry.isSameAllocation(shardRouting);
                        // remove shards where recovery target has changed. This re-initializes shards later in applyNewOrUpdatedShards
                        if (shardRouting.isPeerRecovery()) {
                            RecoveryState recoveryState = existingShard.recoveryState();
                            if (recoveryState != null && recoveryState.getStage() != RecoveryState.Stage.DONE) { // shard is still recovering
                                if (recoveryState.getType() == RecoveryState.Type.PRIMARY_RELOCATION || recoveryState.getType() == RecoveryState.Type.REPLICA) {
                                    final DiscoveryNode sourceNode = findSourceNodeForPeerRecovery(logger, routingTable, nodes, shardRouting);
                                    if (recoveryState.getSourceNode().equals(sourceNode) == false) {
                                        logger.debug("[{}][{}] removing shard (recovery source changed), current [{}], global [{}])", shardRouting.index(), shardRouting.id(), currentRoutingEntry, shardRouting);
                                        // closing the shard will also cancel any ongoing recovery.
                                        indexService.removeShard(shardRouting.id(), "removing shard (recovery source node changed)");
                                    }
                                }
                            }
                        }
                    }
                }

                // if no shards remain for this index in routing nodes, remove index to free up resources
                // this also handles the case when index was closed
                if (newShardAllocationIds.isEmpty()) {
                    assert indexService.shardIds().isEmpty() :
                        "no locally assigned shards, but index wasn't emptied earlier."
                            + " index " + index + ", shards: " + indexService.shardIds();
                    if (logger.isDebugEnabled()) {
                        logger.debug("{} cleaning index (no shards allocated)", index);
                    }
                    // clean the index
                    removeIndex(index, "removing index (no shards allocated)");
                }
            }
        }
    }

    private void applyNewIndices(final ClusterChangedEvent event) {
        // we only create indices for shards that are allocated
        RoutingNode localRoutingNode = event.state().getRoutingNodes().node(event.state().nodes().getLocalNodeId());
        if (localRoutingNode == null) {
            return;
        }
        Set<Index> failedIndices = new HashSet<>(); // indices where creation failed (to make sure we only try once for each index)
        for (ShardRouting shard : localRoutingNode) {
            Index index = shard.index();
            if (failedShardsCache.containsKey(shard.shardId())) {
                // ignore
                continue;
            }
            if (failedIndices.contains(index)) {
                // if we failed the mappings anywhere, we need to fail the shards for this index, note, we safeguard
                // by creating the processing the mappings on the master, or on the node the mapping was introduced on,
                // so this failure typically means wrong node level configuration or something similar
                sendFailShard(shard, "failed to create index (for shard with same index)", null);
            } else {
                if (!indicesService.hasIndex(index)) {
                    final IndexMetaData indexMetaData = event.state().metaData().index(shard.index());
                    if (logger.isDebugEnabled()) {
                        logger.debug("[{}] creating index", indexMetaData.getIndex());
                    }
                    IndexService indexService = null;
                    try {
                        indexService = indicesService.createIndex(nodeServicesProvider, indexMetaData, buildInIndexListener);
                    } catch (Throwable e) {
                        // if we failed the mappings anywhere, we need to fail the shards for this index, note, we safeguard
                        // by creating the processing the mappings on the master, or on the node the mapping was introduced on,
                        // so this failure typically means wrong node level configuration or something similar
                        sendFailShard(shard, "failed to create index", e);
                        failedIndices.add(index);
                    }
                    if (indexService != null) {
                        try {
                            updateIndexMapping(event, indexMetaData, index, indexService);
                        } catch (Throwable e) {
                            removeIndex(index, "removing newly created index (mapping update failed)");
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

    private void applyUpdateExistingIndices(ClusterChangedEvent event) {
        if (!event.metaDataChanged()) {
            return;
        }
        for (IndexMetaData indexMetaData : event.state().metaData()) {
            Index index = indexMetaData.getIndex();
            IndexService indexService = indicesService.indexService(index);
            if (indexService == null) {
                continue;
            }
            // if the index meta data didn't change, no need check for refreshed settings
            if (!event.indexMetaDataChanged(indexMetaData)) {
                continue;
            }
            indexService.updateMetaData(indexMetaData);
            try {
                updateIndexMapping(event, indexMetaData, index, indexService);
            } catch (Throwable t) {
                // if we failed the mappings anywhere, we need to fail the shards for this index, note, we safeguard
                // by creating the processing the mappings on the master, or on the node the mapping was introduced on,
                // so this failure typically means wrong node level configuration or something similar
                for (IndexShard indexShard : indexService) {
                    failAndRemoveShard(indexShard.routingEntry(), indexService, true, "failed to update mappings", t);
                }
            }
        }
    }

    private void updateIndexMapping(ClusterChangedEvent event, IndexMetaData indexMetaData, Index index, IndexService indexService) throws Throwable {
        MapperService mapperService = indexService.mapperService();
        // go over and add the relevant mappings (or update them)
        boolean requireRefresh = false;
        for (ObjectCursor<MappingMetaData> cursor : indexMetaData.getMappings().values()) {
            MappingMetaData mappingMd = cursor.value;
            String mappingType = mappingMd.type();
            CompressedXContent mappingSource = mappingMd.source();
            // refresh mapping can happen when the parsing/merging of the mapping from the metadata doesn't result in the same
            // mapping, in this case, we send to the master to refresh its own version of the mappings (to conform with the
            // merge version of it, which it does when refreshing the mappings), and warn log it.
            try {
                DocumentMapper existingMapper = mapperService.documentMapper(mappingType);

                if (existingMapper == null || mappingSource.equals(existingMapper.mappingSource()) == false) {
                    String op = existingMapper == null ? "adding" : "updating";
                    if (logger.isDebugEnabled() && mappingSource.compressed().length < 512) {
                        logger.debug("[{}] {} mapping [{}], source [{}]", index, op, mappingType, mappingSource.string());
                    } else if (logger.isTraceEnabled()) {
                        logger.trace("[{}] {} mapping [{}], source [{}]", index, op, mappingType, mappingSource.string());
                    } else {
                        logger.debug("[{}] {} mapping [{}] (source suppressed due to length, use TRACE level if needed)", index, op, mappingType);
                    }
                    mapperService.merge(mappingType, mappingSource, MapperService.MergeReason.MAPPING_RECOVERY, true);
                    if (!mapperService.documentMapper(mappingType).mappingSource().equals(mappingSource)) {
                        logger.debug("[{}] parsed mapping [{}], and got different sources\noriginal:\n{}\nparsed:\n{}", index, mappingType, mappingSource, mapperService.documentMapper(mappingType).mappingSource());
                        requireRefresh = true;
                    }
                }
            } catch (Throwable e) {
                logger.warn("[{}] failed to add mapping [{}], source [{}]", e, index, mappingType, mappingSource);
                throw e;
            }
        }
        if (requireRefresh && sendRefreshMapping) {
            nodeMappingRefreshAction.nodeMappingRefresh(event.state(),
                new NodeMappingRefreshAction.NodeMappingRefreshRequest(index.getName(), indexMetaData.getIndexUUID(),
                    event.state().nodes().getLocalNodeId())
            );
        }
    }

    private void applyNewOrUpdatedShards(final ClusterChangedEvent event) {
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
            final IndexService indexService = indicesService.indexService(shardRouting.index());
            if (indexService == null) {
                // creation failed for some reasons
                assert failedShardsCache.containsKey(shardRouting.shardId()) :
                    "index has local allocation but is not created by applyNewIndices and is not failed " + shardRouting;
                continue;
            }
            createOrUpdateShard(event, nodes, routingTable, shardRouting, indexService);
        }
    }

    private void createOrUpdateShard(ClusterChangedEvent event, DiscoveryNodes nodes, RoutingTable routingTable, ShardRouting shardRouting, IndexService indexService) {
        final IndexMetaData indexMetaData = event.state().metaData().index(shardRouting.index());
        assert indexMetaData != null : "index has local allocation but no meta data. " + shardRouting.index();

        final int shardId = shardRouting.id();

        IndexShard indexShard = indexService.getShardOrNull(shardId);
        if (indexShard != null) {
            ShardRouting currentRoutingEntry = indexShard.routingEntry();
            assert currentRoutingEntry.isSameAllocation(shardRouting) :
                "local shard has a different allocation id but wasn't cleaning by applyDeletedShards. "
                    + "cluster state: " + shardRouting + " local: " + currentRoutingEntry;
            try {
                indexShard.updateRoutingEntry(shardRouting, event.state().blocks().disableStatePersistence() == false);
            } catch (Throwable e) {
                failAndRemoveShard(shardRouting, indexService, true, "failed updating shard routing entry", e);
                return;
            }

            IndexShardState state = indexShard.state();

            if (shardRouting.initializing() && (state == IndexShardState.STARTED || state == IndexShardState.POST_RECOVERY)) {
                // the master thinks we are initializing, but we are already started or on POST_RECOVERY and waiting
                // for master to confirm a shard started message (either master failover, or a cluster event before
                // we managed to tell the master we started), mark us as started
                if (logger.isTraceEnabled()) {
                    logger.trace("{} master marked shard as initializing, but shard has state [{}], resending shard started to {}",
                        indexShard.shardId(), indexShard.state(), nodes.getMasterNode());
                }
                if (nodes.getMasterNode() != null) {
                    shardStateAction.shardStarted(shardRouting,
                        "master " + nodes.getMasterNode() + " marked shard as initializing, but shard state is [" + indexShard.state() + "], mark shard as started",
                        SHARD_STATE_ACTION_LISTENER);
                }
                return;
            }
            if (indexShard.ignoreRecoveryAttempt()) {
                logger.trace("ignoring recovery instruction for an existing shard {} (shard state: [{}])", indexShard.shardId(), indexShard.state());
                return;
            }
            assert state == IndexShardState.CREATED;
        }

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

            // if there is no shard, create it
            if (indexShard == null) {
                try {
                    if (logger.isDebugEnabled()) {
                        logger.debug("[{}][{}] creating shard", shardRouting.index(), shardId);
                    }
                    indexShard = indexService.createShard(shardRouting);
                    indexShard.addShardFailureCallback(failedShardHandler);
                } catch (IndexShardAlreadyExistsException e) {
                    // ignore this, the method call can happen several times
                } catch (Throwable e) {
                    failAndRemoveShard(shardRouting, indexService, true, "failed to create shard", e);
                    return;
                }
            }

            assert indexShard != null;

            if (indexShard.ignoreRecoveryAttempt()) {
                // we are already recovering (we can get to this state since the cluster event can happen several
                // times while we recover)
                logger.trace("ignoring recovery instruction for shard {} (shard state: [{}])", indexShard.shardId(), indexShard.state());
            } else {
                indexShard.startRecovery(shardRouting, nodes.getLocalNode(), sourceNode, recoveryTargetService,
                    new RecoveryListener(shardRouting, indexService), repositoriesService);
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
        private final IndexService indexService;

        private RecoveryListener(ShardRouting shardRouting, IndexService indexService) {
            this.shardRouting = shardRouting;
            this.indexService = indexService;
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
                    handleRecoveryFailure(indexService, shardRouting, sendShardFailure, e);
                }
            } else {
                handleRecoveryFailure(indexService, shardRouting, sendShardFailure, e);
            }
        }
    }

    private void handleRecoveryFailure(IndexService indexService, ShardRouting shardRouting, boolean sendShardFailure, Throwable failure) {
        synchronized (mutex) {
            failAndRemoveShard(shardRouting, indexService, sendShardFailure, "failed recovery", failure);
        }
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

    private void failAndRemoveShard(ShardRouting shardRouting, @Nullable IndexService indexService, boolean sendShardFailure, String message, @Nullable Throwable failure) {
        if (indexService != null && indexService.hasShard(shardRouting.getId())) {
            // if the indexService is null we can't remove the shard, that's fine since we might have a failure
            // when the index is remove and then we already removed the index service for that shard...
            try {
                indexService.removeShard(shardRouting.getId(), message);
            } catch (ShardNotFoundException e) {
                // the node got closed on us, ignore it
            } catch (Throwable e1) {
                logger.warn("[{}][{}] failed to remove shard after failure ([{}])", e1, shardRouting.getIndexName(), shardRouting.getId(), message);
            }
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
            final IndexService indexService = indicesService.indexService(shardFailure.routing.shardId().getIndex());
            final ShardRouting shardRouting = shardFailure.routing;
            threadPool.generic().execute(() -> {
                synchronized (mutex) {
                    failAndRemoveShard(shardRouting, indexService, true, "shard failure, reason [" + shardFailure.reason + "]", shardFailure.cause);
                }
            });
        }
    }
}
