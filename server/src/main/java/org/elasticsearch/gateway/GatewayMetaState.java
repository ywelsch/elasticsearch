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

package org.elasticsearch.gateway;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaDataIndexUpgradeService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.gateway.MetaStateService.MetaState;
import org.elasticsearch.index.Index;
import org.elasticsearch.plugins.MetaDataUpgrader;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

import static java.util.Collections.unmodifiableMap;

public class GatewayMetaState extends AbstractComponent implements ClusterStateApplier {

    private final NodeEnvironment nodeEnv;
    private final MetaStateService metaStateService;

    @Nullable
    private volatile MetaData previousMetaData;

    private volatile long previousGlobalStateGeneration = -1L;

    private volatile Map<Index, Long> previouslyWrittenIndices = new HashMap<>();

    public GatewayMetaState(Settings settings, NodeEnvironment nodeEnv, MetaStateService metaStateService,
                            MetaDataIndexUpgradeService metaDataIndexUpgradeService, MetaDataUpgrader metaDataUpgrader) throws IOException {
        super(settings);
        this.nodeEnv = nodeEnv;
        this.metaStateService = metaStateService;

        if (DiscoveryNode.isDataNode(settings)) {
            ensureNoPre019ShardState(nodeEnv);
        }

        if (DiscoveryNode.isMasterNode(settings) || DiscoveryNode.isDataNode(settings)) {
            nodeEnv.ensureAtomicMoveSupported();
        }
        if (DiscoveryNode.isMasterNode(settings) || DiscoveryNode.isDataNode(settings)) {
            try {
                ensureNoPre019State();
                final MetaData metaData = metaStateService.loadAtomicFullState();
                final MetaData upgradedMetaData = upgradeMetaData(metaData, metaDataIndexUpgradeService, metaDataUpgrader);
                // We finished global state validation and successfully checked all indices for backward compatibility
                // and found no non-upgradable indices, which means the upgrade can continue.
                // Now it's safe to overwrite global and index metadata.
                if (metaData != upgradedMetaData) {
                    long globalStateGeneration;
                    Map<Index, Long> indices = new HashMap<>();
                    if (MetaData.isGlobalStateEquals(metaData, upgradedMetaData) == false) {
                        globalStateGeneration = metaStateService.writeGlobalState("upgrade", upgradedMetaData);
                    } else {
                        globalStateGeneration = metaStateService.loadGlobalState().v2();
                    }
                    for (IndexMetaData indexMetaData : upgradedMetaData) {
                        if (metaData.hasIndexMetaData(indexMetaData) == false) {
                            indices.put(indexMetaData.getIndex(), metaStateService.writeIndex("upgrade", indexMetaData));
                        }
                    }
                    metaStateService.writeMetaState("upgrade", new MetaState(globalStateGeneration, indices));
                }
                long startNS = System.nanoTime();
                metaStateService.loadAtomicFullState();
                logger.debug("took {} to load state", TimeValue.timeValueMillis(TimeValue.nsecToMSec(System.nanoTime() - startNS)));
            } catch (Exception e) {
                logger.error("failed to read local state, exiting...", e);
                throw e;
            }
        }
    }

    public MetaData loadMetaState() throws IOException {
        return metaStateService.loadAtomicFullState();
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        final ClusterState state = event.state();
        if (state.blocks().disableStatePersistence()) {
            // reset the current metadata, we need to start fresh...
            this.previousMetaData = null;
            previouslyWrittenIndices = new HashMap<>();
            return;
        }

        long globalStateGeneration = previousGlobalStateGeneration;

        MetaData newMetaData = state.metaData();
        // we don't check if metaData changed, since we might be called several times and we need to check dangling...
        Map<Index, Long> newIndices = Collections.emptyMap();
        boolean success = true;
        // write the state if this node is a master eligible node or if it is a data node and has shards allocated on it
        if (state.nodes().getLocalNode().isMasterNode() || state.nodes().getLocalNode().isDataNode()) {
            if (previousMetaData == null) {
                try {
                    // we determine if or if not we write meta data on data only nodes by looking at the shard routing
                    // and only write if a shard of this index is allocated on this node
                    // however, closed indices do not appear in the shard routing. if the meta data for a closed index is
                    // updated it will therefore not be written in case the list of previouslyWrittenIndices is empty (because state
                    // persistence was disabled or the node was restarted), see getRelevantIndicesOnDataOnlyNode().
                    // we therefore have to check here if we have shards on disk and add their indices to the previouslyWrittenIndices list
                    if (isDataOnlyNode(state)) {
                        Map<Index, Long> newPreviouslyWrittenIndices = new HashMap<>(previouslyWrittenIndices);
                        for (IndexMetaData indexMetaData : newMetaData) {
                            if (indexMetaData.getState().equals(IndexMetaData.State.CLOSE)) {
                                Tuple<IndexMetaData, Long> indexMetaDataOnDisk = metaStateService.loadIndexState(indexMetaData.getIndex());
                                if (indexMetaDataOnDisk.v1() != null) {
                                    newPreviouslyWrittenIndices.put(indexMetaDataOnDisk.v1().getIndex(), indexMetaDataOnDisk.v2());
                                }
                            }
                        }
                        previouslyWrittenIndices = newPreviouslyWrittenIndices;
                    }
                } catch (Exception e) {
                    success = false;
                }
            }

            // check if the global state changed?
            if (previousMetaData == null || !MetaData.isGlobalStateEquals(previousMetaData, newMetaData)) {
                try {
                    globalStateGeneration = metaStateService.writeGlobalState("changed", newMetaData);
                } catch (Exception e) {
                    success = false;
                }
            }

            newIndices = new HashMap<>(previouslyWrittenIndices);
            Set<Index> relevantIndices = getRelevantIndices(event.state(), event.previousState(), previouslyWrittenIndices.keySet());
            newIndices.keySet().retainAll(relevantIndices);

            final Iterable<IndexMetaWriteInfo> writeInfo = resolveStatesToBeWritten(previouslyWrittenIndices, relevantIndices,
                previousMetaData, event.state().metaData());
            // check and write changes in indices
            for (IndexMetaWriteInfo indexMetaWrite : writeInfo) {
                try {
                    final long generation;
                    if (indexMetaWrite.shouldWrite()) {
                        generation = metaStateService.writeIndex(indexMetaWrite.reason, indexMetaWrite.newMetaData);
                    } else {
                        generation = indexMetaWrite.previousGeneration();
                    }
                    newIndices.put(indexMetaWrite.newMetaData.getIndex(), generation);
                } catch (Exception e) {
                    success = false;
                }
            }

            if (success) {
                try {
                    // write list of indices with their generations
                    final MetaState metaState = new MetaState(globalStateGeneration, newIndices);
                    metaStateService.writeMetaState("changed", metaState);
                } catch (Exception e) {
                    success = false;
                }
            }
        }

        if (success) {
            previousGlobalStateGeneration = globalStateGeneration;
            previousMetaData = newMetaData;
            previouslyWrittenIndices = unmodifiableMap(newIndices);
        }
    }

    public static Set<Index> getRelevantIndices(ClusterState state, ClusterState previousState, Set<Index> previouslyWrittenIndices) {
        Set<Index> relevantIndices;
        if (isDataOnlyNode(state)) {
            relevantIndices = getRelevantIndicesOnDataOnlyNode(state, previousState, previouslyWrittenIndices);
        } else if (state.nodes().getLocalNode().isMasterNode()) {
            relevantIndices = getRelevantIndicesForMasterEligibleNode(state);
        } else {
            relevantIndices = Collections.emptySet();
        }
        return relevantIndices;
    }


    protected static boolean isDataOnlyNode(ClusterState state) {
        return ((state.nodes().getLocalNode().isMasterNode() == false) && state.nodes().getLocalNode().isDataNode());
    }

    /**
     * Throws an IAE if a pre 0.19 state is detected
     */
    private void ensureNoPre019State() throws IOException {
        for (Path dataLocation : nodeEnv.nodeDataPaths()) {
            final Path stateLocation = dataLocation.resolve(MetaDataStateFormat.STATE_DIR_NAME);
            if (!Files.exists(stateLocation)) {
                continue;
            }
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(stateLocation)) {
                for (Path stateFile : stream) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("[upgrade]: processing [{}]", stateFile.getFileName());
                    }
                    final String name = stateFile.getFileName().toString();
                    if (name.startsWith("metadata-")) {
                        throw new IllegalStateException("Detected pre 0.19 metadata file please upgrade to a version before "
                            + Version.CURRENT.minimumIndexCompatibilityVersion()
                            + " first to upgrade state structures - metadata found: [" + stateFile.getParent().toAbsolutePath());
                    }
                }
            }
        }
    }

    /**
     * Elasticsearch 2.0 removed several deprecated features and as well as support for Lucene 3.x. This method calls
     * {@link MetaDataIndexUpgradeService} to makes sure that indices are compatible with the current version. The
     * MetaDataIndexUpgradeService might also update obsolete settings if needed.
     * Allows upgrading global custom meta data via {@link MetaDataUpgrader#customMetaDataUpgraders}
     *
     * @return input <code>metaData</code> if no upgrade is needed or an upgraded metaData
     */
    static MetaData upgradeMetaData(MetaData metaData,
                                    MetaDataIndexUpgradeService metaDataIndexUpgradeService,
                                    MetaDataUpgrader metaDataUpgrader) throws IOException {
        // upgrade index meta data
        boolean changed = false;
        final MetaData.Builder upgradedMetaData = MetaData.builder(metaData);
        for (IndexMetaData indexMetaData : metaData) {
            IndexMetaData newMetaData = metaDataIndexUpgradeService.upgradeIndexMetaData(indexMetaData,
                Version.CURRENT.minimumIndexCompatibilityVersion());
            changed |= indexMetaData != newMetaData;
            upgradedMetaData.put(newMetaData, false);
        }
        // upgrade global custom meta data
        if (applyPluginUpgraders(metaData.getCustoms(), metaDataUpgrader.customMetaDataUpgraders,
            upgradedMetaData::removeCustom,upgradedMetaData::putCustom)) {
            changed = true;
        }
        // upgrade current templates
        if (applyPluginUpgraders(metaData.getTemplates(), metaDataUpgrader.indexTemplateMetaDataUpgraders,
            upgradedMetaData::removeTemplate, (s, indexTemplateMetaData) -> upgradedMetaData.put(indexTemplateMetaData))) {
            changed = true;
        }
        return changed ? upgradedMetaData.build() : metaData;
    }

    private static <Data> boolean applyPluginUpgraders(ImmutableOpenMap<String, Data> existingData,
                                                       UnaryOperator<Map<String, Data>> upgrader,
                                                       Consumer<String> removeData,
                                                       BiConsumer<String, Data> putData) {
        // collect current data
        Map<String, Data> existingMap = new HashMap<>();
        for (ObjectObjectCursor<String, Data> customCursor : existingData) {
            existingMap.put(customCursor.key, customCursor.value);
        }
        // upgrade global custom meta data
        Map<String, Data> upgradedCustoms = upgrader.apply(existingMap);
        if (upgradedCustoms.equals(existingMap) == false) {
            // remove all data first so a plugin can remove custom metadata or templates if needed
            existingMap.keySet().forEach(removeData);
            for (Map.Entry<String, Data> upgradedCustomEntry : upgradedCustoms.entrySet()) {
                putData.accept(upgradedCustomEntry.getKey(), upgradedCustomEntry.getValue());
            }
            return true;
        }
        return false;
    }

    // shard state BWC
    private void ensureNoPre019ShardState(NodeEnvironment nodeEnv) throws IOException {
        for (Path dataLocation : nodeEnv.nodeDataPaths()) {
            final Path stateLocation = dataLocation.resolve(MetaDataStateFormat.STATE_DIR_NAME);
            if (Files.exists(stateLocation)) {
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(stateLocation, "shards-*")) {
                    for (Path stateFile : stream) {
                        throw new IllegalStateException("Detected pre 0.19 shard state file please upgrade to a version before "
                                + Version.CURRENT.minimumIndexCompatibilityVersion()
                                + " first to upgrade state structures - shard state found: [" + stateFile.getParent().toAbsolutePath());
                    }
                }
            }
        }
    }

    /**
     * Loads the current meta state for each index in the new cluster state and checks if it has to be persisted.
     * Each index state that should be written to disk will be returned. This is only run for data only nodes.
     * It will return only the states for indices that actually have a shard allocated on the current node.
     *
     * @param previouslyWrittenIndices    A list of indices for which the state was already written before
     * @param potentiallyUnwrittenIndices The list of indices for which state should potentially be written
     * @param previousMetaData            The last meta data we know of. meta data for all indices in previouslyWrittenIndices list is persisted now
     * @param newMetaData                 The new metadata
     * @return iterable over all indices states that should be written to disk
     */
    public static List<GatewayMetaState.IndexMetaWriteInfo> resolveStatesToBeWritten(Map<Index, Long> previouslyWrittenIndices, Set<Index> potentiallyUnwrittenIndices, MetaData previousMetaData, MetaData newMetaData) {
        List<GatewayMetaState.IndexMetaWriteInfo> indicesToWrite = new ArrayList<>();
        for (Index index : potentiallyUnwrittenIndices) {
            IndexMetaData newIndexMetaData = newMetaData.getIndexSafe(index);
            IndexMetaData previousIndexMetaData = previousMetaData == null ? null : previousMetaData.index(index);
            String writeReason = null;
            if (previouslyWrittenIndices.containsKey(index) == false || previousIndexMetaData == null) {
                writeReason = "freshly created";
            } else if (previousIndexMetaData.getVersion() != newIndexMetaData.getVersion()) {
                writeReason = "version changed from [" + previousIndexMetaData.getVersion() + "] to [" + newIndexMetaData.getVersion() + "]";
            }
            indicesToWrite.add(new GatewayMetaState.IndexMetaWriteInfo(newIndexMetaData, previousIndexMetaData, writeReason, previouslyWrittenIndices.getOrDefault(index, -1L)));
        }
        return indicesToWrite;
    }

    public static Set<Index> getRelevantIndicesOnDataOnlyNode(ClusterState state, ClusterState previousState, Set<Index> previouslyWrittenIndices) {
        RoutingNode newRoutingNode = state.getRoutingNodes().node(state.nodes().getLocalNodeId());
        if (newRoutingNode == null) {
            throw new IllegalStateException("cluster state does not contain this node - cannot write index meta state");
        }
        Set<Index> indices = new HashSet<>();
        for (ShardRouting routing : newRoutingNode) {
            indices.add(routing.index());
        }
        // we have to check the meta data also: closed indices will not appear in the routing table, but we must still write the state if we have it written on disk previously
        for (IndexMetaData indexMetaData : state.metaData()) {
            boolean isOrWasClosed = indexMetaData.getState().equals(IndexMetaData.State.CLOSE);
            // if the index is open we might still have to write the state if it just transitioned from closed to open
            // so we have to check for that as well.
            IndexMetaData previousMetaData = previousState.metaData().index(indexMetaData.getIndex());
            if (previousMetaData != null) {
                isOrWasClosed = isOrWasClosed || previousMetaData.getState().equals(IndexMetaData.State.CLOSE);
            }
            if (previouslyWrittenIndices.contains(indexMetaData.getIndex()) && isOrWasClosed) {
                indices.add(indexMetaData.getIndex());
            }
        }
        return indices;
    }

    public static Set<Index> getRelevantIndicesForMasterEligibleNode(ClusterState state) {
        Set<Index> relevantIndices;
        relevantIndices = new HashSet<>();
        // we have to iterate over the metadata to make sure we also capture closed indices
        for (IndexMetaData indexMetaData : state.metaData()) {
            relevantIndices.add(indexMetaData.getIndex());
        }
        return relevantIndices;
    }


    public static class IndexMetaWriteInfo {
        final IndexMetaData newMetaData;
        @Nullable final String reason;
        final IndexMetaData previousMetaData;
        final long previousGeneration;

        public IndexMetaWriteInfo(IndexMetaData newMetaData, IndexMetaData previousMetaData, @Nullable String reason,
                                  long previousGeneration) {
            this.newMetaData = newMetaData;
            this.reason = reason;
            this.previousMetaData = previousMetaData;
            this.previousGeneration = previousGeneration;
        }

        public IndexMetaData getNewMetaData() {
            return newMetaData;
        }

        public String getReason() {
            return reason;
        }

        public boolean shouldWrite() {
            return reason != null;
        }

        public long previousGeneration() {
            return previousGeneration;
        }
    }
}
