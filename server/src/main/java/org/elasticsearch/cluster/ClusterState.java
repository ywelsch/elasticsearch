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

package org.elasticsearch.cluster;

import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.zen.PublishClusterStateAction;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Represents the current state of the cluster.
 * <p>
 * The cluster state object is immutable with the exception of the {@link RoutingNodes} structure, which is
 * built on demand from the {@link RoutingTable}.
 * The cluster state can be updated only on the master node. All updates are performed by on a
 * single thread and controlled by the {@link ClusterService}. After every update the
 * {@link Discovery#publish} method publishes a new version of the cluster state to all other nodes in the
 * cluster.  The actual publishing mechanism is delegated to the {@link Discovery#publish} method and depends on
 * the type of discovery. In the Zen Discovery it is handled in the {@link PublishClusterStateAction#publish} method. The
 * publishing mechanism can be overridden by other discovery.
 * <p>
 * The cluster state implements the {@link Diffable} interface in order to support publishing of cluster state
 * differences instead of the entire state on each change. The publishing mechanism should only send differences
 * to a node if this node was present in the previous version of the cluster state. If a node was
 * not present in the previous version of the cluster state, this node is unlikely to have the previous cluster
 * state version and should be sent a complete version. In order to make sure that the differences are applied to the
 * correct version of the cluster state, each cluster state version update generates {@link #stateUUID} that uniquely
 * identifies this version of the state. This uuid is verified by the {@link ClusterStateDiff#apply} method to
 * make sure that the correct diffs are applied. If uuids don’t match, the {@link ClusterStateDiff#apply} method
 * throws the {@link IncompatibleClusterStateVersionException}, which causes the publishing mechanism to send
 * a full version of the cluster state to the node on which this exception was thrown.
 */
public class ClusterState implements ToXContentFragment, Diffable<ClusterState> {

    public static final ClusterState EMPTY_STATE = builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).build();

    public interface Custom extends NamedDiffable<Custom>, ToXContentFragment {

        /**
         * Returns <code>true</code> iff this {@link Custom} is private to the cluster and should never be send to a client.
         * The default is <code>false</code>;
         */
        default boolean isPrivate() {
            return false;
        }
    }

    private static final NamedDiffableValueSerializer<Custom> CUSTOM_VALUE_SERIALIZER = new NamedDiffableValueSerializer<>(Custom.class);

    public static final String UNKNOWN_UUID = "_na_";

    public static final long UNKNOWN_VERSION = -1;

    private final long version;

    private final long term;

    private final String stateUUID;

    private final RoutingTable routingTable;

    private final DiscoveryNodes nodes;

    private final MetaData metaData;

    private final ClusterBlocks blocks;

    private final ImmutableOpenMap<String, Custom> customs;

    private final ClusterName clusterName;

    private final boolean wasReadFromDiff;

    private final VotingConfiguration lastCommittedConfiguration;

    private final VotingConfiguration lastAcceptedConfiguration;

    // built on demand
    private volatile RoutingNodes routingNodes;

    public ClusterState(long version, long term, String stateUUID, ClusterState state) {
        this(state.clusterName, version, term, stateUUID, state.metaData(), state.routingTable(), state.nodes(), state.blocks(),
            state.customs(), state.getLastCommittedConfiguration(), state.getLastAcceptedConfiguration(), false);
    }

    public ClusterState(ClusterName clusterName, long version, long term, String stateUUID, MetaData metaData, RoutingTable routingTable,
                        DiscoveryNodes nodes, ClusterBlocks blocks, ImmutableOpenMap<String, Custom> customs,
                        VotingConfiguration lastCommittedConfiguration, VotingConfiguration lastAcceptedConfiguration,
                        boolean wasReadFromDiff) {
        this.version = version;
        this.term = term;
        this.stateUUID = stateUUID;
        this.clusterName = clusterName;
        this.metaData = metaData;
        this.routingTable = routingTable;
        this.nodes = nodes;
        this.blocks = blocks;
        this.customs = customs;
        this.lastCommittedConfiguration = lastCommittedConfiguration;
        this.lastAcceptedConfiguration = lastAcceptedConfiguration;
        this.wasReadFromDiff = wasReadFromDiff;
    }

    public long version() {
        return this.version;
    }

    public long getVersion() {
        return version();
    }

    public long term() {
        return term;
    }

    /**
     * This stateUUID is automatically generated for for each version of cluster state. It is used to make sure that
     * we are applying diffs to the right previous state.
     */
    public String stateUUID() {
        return this.stateUUID;
    }

    public DiscoveryNodes nodes() {
        return this.nodes;
    }

    public DiscoveryNodes getNodes() {
        return nodes();
    }

    public MetaData metaData() {
        return this.metaData;
    }

    public MetaData getMetaData() {
        return metaData();
    }

    public RoutingTable routingTable() {
        return routingTable;
    }

    public RoutingTable getRoutingTable() {
        return routingTable();
    }

    public ClusterBlocks blocks() {
        return this.blocks;
    }

    public ClusterBlocks getBlocks() {
        return blocks;
    }

    public ImmutableOpenMap<String, Custom> customs() {
        return this.customs;
    }

    public ImmutableOpenMap<String, Custom> getCustoms() {
        return this.customs;
    }

    public <T extends Custom> T custom(String type) {
        return (T) customs.get(type);
    }

    public ClusterName getClusterName() {
        return this.clusterName;
    }

    public VotingConfiguration getLastAcceptedConfiguration() {
        return lastAcceptedConfiguration;
    }

    public VotingConfiguration getLastCommittedConfiguration() {
        return lastCommittedConfiguration;
    }

    // Used for testing and logging to determine how this cluster state was send over the wire
    public boolean wasReadFromDiff() {
        return wasReadFromDiff;
    }

    /**
     * Returns a built (on demand) routing nodes view of the routing table.
     */
    public RoutingNodes getRoutingNodes() {
        if (routingNodes != null) {
            return routingNodes;
        }
        routingNodes = new RoutingNodes(this);
        return routingNodes;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("cluster uuid: ").append(metaData.clusterUUID()).append("\n");
        sb.append("version: ").append(version).append("\n");
        sb.append("term: ").append(term).append("\n");
        sb.append("state uuid: ").append(stateUUID).append("\n");
        sb.append("from_diff: ").append(wasReadFromDiff).append("\n");
        sb.append("meta data version: ").append(metaData.version()).append("\n");
        final String TAB = "   ";
        for (IndexMetaData indexMetaData : metaData) {
            sb.append(TAB).append(indexMetaData.getIndex());
            sb.append(": v[").append(indexMetaData.getVersion()).append("]\n");
            for (int shard = 0; shard < indexMetaData.getNumberOfShards(); shard++) {
                sb.append(TAB).append(TAB).append(shard).append(": ");
                sb.append("p_term [").append(indexMetaData.primaryTerm(shard)).append("], ");
                sb.append("isa_ids ").append(indexMetaData.inSyncAllocationIds(shard)).append("\n");
            }
        }
        sb.append(blocks());
        sb.append(nodes());
        sb.append("last committed conf: ").append(getLastCommittedConfiguration()).append("\n");
        sb.append("last accepted conf: ").append(getLastAcceptedConfiguration()).append("\n");
        sb.append(routingTable());
        sb.append(getRoutingNodes());
        if (customs.isEmpty() == false) {
            sb.append("customs:\n");
            for (ObjectObjectCursor<String, Custom> cursor : customs) {
                final String type = cursor.key;
                final Custom custom = cursor.value;
                sb.append(TAB).append(type).append(": ").append(custom);
            }
        }
        return sb.toString();
    }

    /**
     * a cluster state supersedes another state if they are from the same master and the version of this state is higher than that of the
     * other state.
     * <p>
     * In essence that means that all the changes from the other cluster state are also reflected by the current one
     */
    public boolean supersedes(ClusterState other) {
        return this.nodes().getMasterNodeId() != null && this.nodes().getMasterNodeId().equals(other.nodes().getMasterNodeId())
            && this.version() > other.version();

    }

    public enum Metric {
        VERSION("version"),
        MASTER_NODE("master_node"),
        BLOCKS("blocks"),
        NODES("nodes"),
        METADATA("metadata"),
        ROUTING_TABLE("routing_table"),
        ROUTING_NODES("routing_nodes"),
        CUSTOMS("customs");

        private static Map<String, Metric> valueToEnum;

        static {
            valueToEnum = new HashMap<>();
            for (Metric metric : Metric.values()) {
                valueToEnum.put(metric.value, metric);
            }
        }

        private final String value;

        Metric(String value) {
            this.value = value;
        }

        public static EnumSet<Metric> parseString(String param, boolean ignoreUnknown) {
            String[] metrics = Strings.splitStringByCommaToArray(param);
            EnumSet<Metric> result = EnumSet.noneOf(Metric.class);
            for (String metric : metrics) {
                if ("_all".equals(metric)) {
                    result = EnumSet.allOf(Metric.class);
                    break;
                }
                Metric m = valueToEnum.get(metric);
                if (m == null) {
                    if (!ignoreUnknown) {
                        throw new IllegalArgumentException("Unknown metric [" + metric + "]");
                    }
                } else {
                    result.add(m);
                }
            }
            return result;
        }

        @Override
        public String toString() {
            return value;
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        EnumSet<Metric> metrics = Metric.parseString(params.param("metric", "_all"), true);

        // always provide the cluster_uuid as part of the top-level response (also part of the metadata response)
        builder.field("cluster_uuid", metaData().clusterUUID());

        if (metrics.contains(Metric.VERSION)) {
            builder.field("version", version);
            builder.field("term", term);
            builder.field("state_uuid", stateUUID);
        }

        if (metrics.contains(Metric.MASTER_NODE)) {
            builder.field("master_node", nodes().getMasterNodeId());
        }

        if (metrics.contains(Metric.BLOCKS)) {
            builder.startObject("blocks");

            if (!blocks().global().isEmpty()) {
                builder.startObject("global");
                for (ClusterBlock block : blocks().global()) {
                    block.toXContent(builder, params);
                }
                builder.endObject();
            }

            if (!blocks().indices().isEmpty()) {
                builder.startObject("indices");
                for (ObjectObjectCursor<String, Set<ClusterBlock>> entry : blocks().indices()) {
                    builder.startObject(entry.key);
                    for (ClusterBlock block : entry.value) {
                        block.toXContent(builder, params);
                    }
                    builder.endObject();
                }
                builder.endObject();
            }

            builder.endObject();
        }

        // nodes
        if (metrics.contains(Metric.NODES)) {
            builder.startObject("nodes");
            for (DiscoveryNode node : nodes) {
                node.toXContent(builder, params);
            }
            builder.endObject();
        }

        // meta data
        if (metrics.contains(Metric.METADATA)) {
            builder.startObject("metadata");
            builder.field("cluster_uuid", metaData().clusterUUID());
            builder.startObject("templates");
            for (ObjectCursor<IndexTemplateMetaData> cursor : metaData().templates().values()) {
                IndexTemplateMetaData templateMetaData = cursor.value;
                builder.startObject(templateMetaData.name());

                builder.field("index_patterns", templateMetaData.patterns());
                builder.field("order", templateMetaData.order());

                builder.startObject("settings");
                Settings settings = templateMetaData.settings();
                settings.toXContent(builder, params);
                builder.endObject();

                builder.startObject("mappings");
                for (ObjectObjectCursor<String, CompressedXContent> cursor1 : templateMetaData.mappings()) {
                    Map<String, Object> mapping = XContentHelper.convertToMap(new BytesArray(cursor1.value.uncompressed()), false).v2();
                    if (mapping.size() == 1 && mapping.containsKey(cursor1.key)) {
                        // the type name is the root value, reduce it
                        mapping = (Map<String, Object>) mapping.get(cursor1.key);
                    }
                    builder.field(cursor1.key);
                    builder.map(mapping);
                }
                builder.endObject();


                builder.endObject();
            }
            builder.endObject();

            builder.startObject("indices");
            for (IndexMetaData indexMetaData : metaData()) {
                builder.startObject(indexMetaData.getIndex().getName());

                builder.field("state", indexMetaData.getState().toString().toLowerCase(Locale.ENGLISH));

                builder.startObject("settings");
                Settings settings = indexMetaData.getSettings();
                settings.toXContent(builder, params);
                builder.endObject();

                builder.startObject("mappings");
                for (ObjectObjectCursor<String, MappingMetaData> cursor : indexMetaData.getMappings()) {
                    Map<String, Object> mapping = XContentHelper
                            .convertToMap(new BytesArray(cursor.value.source().uncompressed()), false).v2();
                    if (mapping.size() == 1 && mapping.containsKey(cursor.key)) {
                        // the type name is the root value, reduce it
                        mapping = (Map<String, Object>) mapping.get(cursor.key);
                    }
                    builder.field(cursor.key);
                    builder.map(mapping);
                }
                builder.endObject();

                builder.startArray("aliases");
                for (ObjectCursor<String> cursor : indexMetaData.getAliases().keys()) {
                    builder.value(cursor.value);
                }
                builder.endArray();

                builder.startObject(IndexMetaData.KEY_PRIMARY_TERMS);
                for (int shard = 0; shard < indexMetaData.getNumberOfShards(); shard++) {
                    builder.field(Integer.toString(shard), indexMetaData.primaryTerm(shard));
                }
                builder.endObject();

                builder.startObject(IndexMetaData.KEY_IN_SYNC_ALLOCATIONS);
                for (IntObjectCursor<Set<String>> cursor : indexMetaData.getInSyncAllocationIds()) {
                    builder.startArray(String.valueOf(cursor.key));
                    for (String allocationId : cursor.value) {
                        builder.value(allocationId);
                    }
                    builder.endArray();
                }
                builder.endObject();

                // index metadata
                builder.endObject();
            }
            builder.endObject();

            for (ObjectObjectCursor<String, MetaData.Custom> cursor : metaData.customs()) {
                builder.startObject(cursor.key);
                cursor.value.toXContent(builder, params);
                builder.endObject();
            }

            builder.endObject();
        }

        // routing table
        if (metrics.contains(Metric.ROUTING_TABLE)) {
            builder.startObject("routing_table");
            builder.startObject("indices");
            for (IndexRoutingTable indexRoutingTable : routingTable()) {
                builder.startObject(indexRoutingTable.getIndex().getName());
                builder.startObject("shards");
                for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                    builder.startArray(Integer.toString(indexShardRoutingTable.shardId().id()));
                    for (ShardRouting shardRouting : indexShardRoutingTable) {
                        shardRouting.toXContent(builder, params);
                    }
                    builder.endArray();
                }
                builder.endObject();
                builder.endObject();
            }
            builder.endObject();
            builder.endObject();
        }

        // routing nodes
        if (metrics.contains(Metric.ROUTING_NODES)) {
            builder.startObject("routing_nodes");
            builder.startArray("unassigned");
            for (ShardRouting shardRouting : getRoutingNodes().unassigned()) {
                shardRouting.toXContent(builder, params);
            }
            builder.endArray();

            builder.startObject("nodes");
            for (RoutingNode routingNode : getRoutingNodes()) {
                builder.startArray(routingNode.nodeId() == null ? "null" : routingNode.nodeId());
                for (ShardRouting shardRouting : routingNode) {
                    shardRouting.toXContent(builder, params);
                }
                builder.endArray();
            }
            builder.endObject();

            builder.endObject();
        }
        if (metrics.contains(Metric.CUSTOMS)) {
            for (ObjectObjectCursor<String, Custom> cursor : customs) {
                builder.startObject(cursor.key);
                cursor.value.toXContent(builder, params);
                builder.endObject();
            }
        }

        return builder;
    }

    public static Builder builder(ClusterName clusterName) {
        return new Builder(clusterName);
    }

    public static Builder builder(ClusterState state) {
        return new Builder(state);
    }

    public static class Builder {

        private final ClusterName clusterName;
        private long version = 0;
        private long term = 0;
        private String uuid = UNKNOWN_UUID;
        private VotingConfiguration lastCommittedConfiguration = VotingConfiguration.EMPTY_CONFIG;
        private VotingConfiguration lastAcceptedConfiguration = VotingConfiguration.EMPTY_CONFIG;
        private MetaData metaData = MetaData.EMPTY_META_DATA;
        private RoutingTable routingTable = RoutingTable.EMPTY_ROUTING_TABLE;
        private DiscoveryNodes nodes = DiscoveryNodes.EMPTY_NODES;
        private ClusterBlocks blocks = ClusterBlocks.EMPTY_CLUSTER_BLOCK;
        private final ImmutableOpenMap.Builder<String, Custom> customs;
        private boolean fromDiff;


        public Builder(ClusterState state) {
            this.clusterName = state.clusterName;
            this.version = state.version();
            this.term = state.term();
            this.uuid = state.stateUUID();
            this.lastCommittedConfiguration = state.getLastCommittedConfiguration();
            this.lastAcceptedConfiguration = state.getLastAcceptedConfiguration();
            this.nodes = state.nodes();
            this.routingTable = state.routingTable();
            this.metaData = state.metaData();
            this.blocks = state.blocks();
            this.customs = ImmutableOpenMap.builder(state.customs());
            this.fromDiff = false;
        }

        public Builder(ClusterName clusterName) {
            customs = ImmutableOpenMap.builder();
            this.clusterName = clusterName;
        }

        public Builder nodes(DiscoveryNodes.Builder nodesBuilder) {
            return nodes(nodesBuilder.build());
        }

        public Builder nodes(DiscoveryNodes nodes) {
            this.nodes = nodes;
            return this;
        }

        public DiscoveryNodes nodes() {
            return nodes;
        }

        public Builder routingTable(RoutingTable routingTable) {
            this.routingTable = routingTable;
            return this;
        }

        public Builder metaData(MetaData.Builder metaDataBuilder) {
            return metaData(metaDataBuilder.build());
        }

        public Builder metaData(MetaData metaData) {
            this.metaData = metaData;
            return this;
        }

        public Builder blocks(ClusterBlocks.Builder blocksBuilder) {
            return blocks(blocksBuilder.build());
        }

        public Builder blocks(ClusterBlocks blocks) {
            this.blocks = blocks;
            return this;
        }

        public Builder version(long version) {
            this.version = version;
            return this;
        }

        public Builder term(long term) {
            this.term = term;
            return this;
        }

        public Builder incrementVersion() {
            this.version = version + 1;
            this.uuid = UNKNOWN_UUID;
            return this;
        }

        public Builder stateUUID(String uuid) {
            this.uuid = uuid;
            return this;
        }

        public Builder lastCommittedConfiguration(VotingConfiguration config) {
            this.lastCommittedConfiguration = config;
            return this;
        }

        public Builder lastAcceptedConfiguration(VotingConfiguration config) {
            this.lastAcceptedConfiguration = config;
            return this;
        }

        public Builder putCustom(String type, Custom custom) {
            customs.put(type, custom);
            return this;
        }

        public Builder removeCustom(String type) {
            customs.remove(type);
            return this;
        }

        public Builder customs(ImmutableOpenMap<String, Custom> customs) {
            this.customs.putAll(customs);
            return this;
        }

        public Builder fromDiff(boolean fromDiff) {
            this.fromDiff = fromDiff;
            return this;
        }

        public ClusterState build() {
            if (UNKNOWN_UUID.equals(uuid)) {
                uuid = UUIDs.randomBase64UUID();
            }
            return new ClusterState(clusterName, version, term, uuid, metaData, routingTable, nodes, blocks, customs.build(),
                lastCommittedConfiguration, lastAcceptedConfiguration, fromDiff);
        }

        public static byte[] toBytes(ClusterState state) throws IOException {
            BytesStreamOutput os = new BytesStreamOutput();
            state.writeTo(os);
            return BytesReference.toBytes(os.bytes());
        }

        /**
         * @param data      input bytes
         * @param localNode used to set the local node in the cluster state.
         */
        public static ClusterState fromBytes(byte[] data, DiscoveryNode localNode, NamedWriteableRegistry registry) throws IOException {
            StreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(data), registry);
            return readFrom(in, localNode);

        }
    }

    @Override
    public Diff<ClusterState> diff(ClusterState previousState) {
        return new ClusterStateDiff(previousState, this);
    }

    public static Diff<ClusterState> readDiffFrom(StreamInput in, DiscoveryNode localNode) throws IOException {
        return new ClusterStateDiff(in, localNode);
    }

    public static ClusterState readFrom(StreamInput in, DiscoveryNode localNode) throws IOException {
        ClusterName clusterName = new ClusterName(in);
        Builder builder = new Builder(clusterName);
        builder.version = in.readLong();
        if (in.getVersion().onOrAfter(Version.V_7_0_0_alpha1)) {
            builder.term = in.readLong();
        }
        builder.uuid = in.readString();
        if (in.getVersion().onOrAfter(Version.V_7_0_0_alpha1)) {
            builder.lastCommittedConfiguration(new VotingConfiguration(in));
            builder.lastAcceptedConfiguration(new VotingConfiguration(in));
        }
        builder.metaData = MetaData.readFrom(in);
        builder.routingTable = RoutingTable.readFrom(in);
        builder.nodes = DiscoveryNodes.readFrom(in, localNode);
        builder.blocks = new ClusterBlocks(in);
        int customSize = in.readVInt();
        for (int i = 0; i < customSize; i++) {
            Custom customIndexMetaData = in.readNamedWriteable(Custom.class);
            builder.putCustom(customIndexMetaData.getWriteableName(), customIndexMetaData);
        }
        return builder.build();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        clusterName.writeTo(out);
        out.writeLong(version);
        if (out.getVersion().onOrAfter(Version.V_7_0_0_alpha1)) {
            out.writeLong(term);
        }
        out.writeString(stateUUID);
        if (out.getVersion().onOrAfter(Version.V_7_0_0_alpha1)) {
            lastCommittedConfiguration.writeTo(out);
            lastAcceptedConfiguration.writeTo(out);
        }
        metaData.writeTo(out);
        routingTable.writeTo(out);
        nodes.writeTo(out);
        blocks.writeTo(out);
        // filter out custom states not supported by the other node
        int numberOfCustoms = 0;
        for (ObjectCursor<Custom> cursor : customs.values()) {
            if (out.getVersion().onOrAfter(cursor.value.getMinimalSupportedVersion())) {
                numberOfCustoms++;
            }
        }
        out.writeVInt(numberOfCustoms);
        for (ObjectCursor<Custom> cursor : customs.values()) {
            if (out.getVersion().onOrAfter(cursor.value.getMinimalSupportedVersion())) {
                out.writeNamedWriteable(cursor.value);
            }
        }
    }

    private static class ClusterStateDiff implements Diff<ClusterState> {

        private final long toVersion;

        private final long toTerm;

        private final String fromUuid;

        private final String toUuid;

        private final ClusterName clusterName;

        private final VotingConfiguration lastCommittedConfiguration;

        private final VotingConfiguration lastAcceptedConfiguration;

        private final Diff<RoutingTable> routingTable;

        private final Diff<DiscoveryNodes> nodes;

        private final Diff<MetaData> metaData;

        private final Diff<ClusterBlocks> blocks;

        private final Diff<ImmutableOpenMap<String, Custom>> customs;

        ClusterStateDiff(ClusterState before, ClusterState after) {
            fromUuid = before.stateUUID;
            toUuid = after.stateUUID;
            toVersion = after.version;
            toTerm = after.term;
            clusterName = after.clusterName;
            lastCommittedConfiguration = after.lastCommittedConfiguration;
            lastAcceptedConfiguration = after.lastAcceptedConfiguration;
            routingTable = after.routingTable.diff(before.routingTable);
            nodes = after.nodes.diff(before.nodes);
            metaData = after.metaData.diff(before.metaData);
            blocks = after.blocks.diff(before.blocks);
            customs = DiffableUtils.diff(before.customs, after.customs, DiffableUtils.getStringKeySerializer(), CUSTOM_VALUE_SERIALIZER);
        }

        ClusterStateDiff(StreamInput in, DiscoveryNode localNode) throws IOException {
            clusterName = new ClusterName(in);
            fromUuid = in.readString();
            toUuid = in.readString();
            toVersion = in.readLong();
            if (in.getVersion().onOrAfter(Version.V_7_0_0_alpha1)) {
                toTerm = in.readLong();
            } else {
                toTerm = 0L;
            }
            if (in.getVersion().onOrAfter(Version.V_7_0_0_alpha1)) {
                lastCommittedConfiguration = new VotingConfiguration(in);
                lastAcceptedConfiguration = new VotingConfiguration(in);
            } else {
                lastCommittedConfiguration = VotingConfiguration.EMPTY_CONFIG;
                lastAcceptedConfiguration = VotingConfiguration.EMPTY_CONFIG;
            }
            routingTable = RoutingTable.readDiffFrom(in);
            nodes = DiscoveryNodes.readDiffFrom(in, localNode);
            metaData = MetaData.readDiffFrom(in);
            blocks = ClusterBlocks.readDiffFrom(in);
            customs = DiffableUtils.readImmutableOpenMapDiff(in, DiffableUtils.getStringKeySerializer(), CUSTOM_VALUE_SERIALIZER);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            clusterName.writeTo(out);
            out.writeString(fromUuid);
            out.writeString(toUuid);
            out.writeLong(toVersion);
            if (out.getVersion().onOrAfter(Version.V_7_0_0_alpha1)) {
                out.writeLong(toTerm);
            }
            if (out.getVersion().onOrAfter(Version.V_7_0_0_alpha1)) {
                lastCommittedConfiguration.writeTo(out);
                lastAcceptedConfiguration.writeTo(out);
            }
            routingTable.writeTo(out);
            nodes.writeTo(out);
            metaData.writeTo(out);
            blocks.writeTo(out);
            customs.writeTo(out);
        }

        @Override
        public ClusterState apply(ClusterState state) {
            Builder builder = new Builder(clusterName);
            if (toUuid.equals(state.stateUUID)) {
                // no need to read the rest - cluster state didn't change
                return state;
            }
            if (fromUuid.equals(state.stateUUID) == false) {
                throw new IncompatibleClusterStateVersionException(state.version, state.stateUUID, toVersion, fromUuid);
            }
            builder.stateUUID(toUuid);
            builder.version(toVersion);
            builder.term(toTerm);
            builder.lastCommittedConfiguration(lastCommittedConfiguration);
            builder.lastAcceptedConfiguration(lastAcceptedConfiguration);
            builder.routingTable(routingTable.apply(state.routingTable));
            builder.nodes(nodes.apply(state.nodes));
            builder.metaData(metaData.apply(state.metaData));
            builder.blocks(blocks.apply(state.blocks));
            builder.customs(customs.apply(state.customs));
            builder.fromDiff(true);
            return builder.build();
        }

    }

    /**
     * A collection of persistent node ids, denoting the voting configuration for cluster state changes.
     */
    public static class VotingConfiguration implements Writeable {

        public static final VotingConfiguration EMPTY_CONFIG = new VotingConfiguration(Collections.emptySet());

        private final Set<String> nodeIds;

        public VotingConfiguration(Set<String> nodeIds) {
            this.nodeIds = Collections.unmodifiableSet(new HashSet<>(nodeIds));
        }

        public VotingConfiguration(StreamInput in) throws IOException {
            nodeIds = Collections.unmodifiableSet(Sets.newHashSet(in.readStringArray()));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeStringArray(nodeIds.toArray(new String[nodeIds.size()]));
        }

        public boolean hasQuorum(Collection<String> votes) {
            if (nodeIds.isEmpty()) {
                return false; // TODO: should we even allow this check on an empty configuration?
            }
            final HashSet<String> intersection = new HashSet<>(nodeIds);
            intersection.retainAll(votes);
            return intersection.size() * 2 > nodeIds.size();
        }

        public Set<String> getNodeIds() {
            return nodeIds;
        }

        @Override
        public String toString() {
            return "VotingConfiguration{" + String.join(",", nodeIds) + "}";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            VotingConfiguration that = (VotingConfiguration) o;
            return Objects.equals(nodeIds, that.nodeIds);
        }

        @Override
        public int hashCode() {
            return Objects.hash(nodeIds);
        }
    }
}
