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

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Handles writing and loading both {@link MetaData} and {@link IndexMetaData}
 */
public class MetaStateService extends AbstractComponent {

    private final NodeEnvironment nodeEnv;
    private final NamedXContentRegistry namedXContentRegistry;

    public MetaStateService(Settings settings, NodeEnvironment nodeEnv, NamedXContentRegistry namedXContentRegistry) {
        super(settings);
        this.nodeEnv = nodeEnv;
        this.namedXContentRegistry = namedXContentRegistry;
    }

    /**
     * Loads the full state, which includes both the global state and all the indices
     * meta state.
     */
    MetaData loadFullState() throws IOException {
        MetaData globalMetaData = loadGlobalState().v1();
        MetaData.Builder metaDataBuilder;
        if (globalMetaData != null) {
            metaDataBuilder = MetaData.builder(globalMetaData);
        } else {
            metaDataBuilder = MetaData.builder();
        }
        for (String indexFolderName : nodeEnv.availableIndexFolders()) {
            IndexMetaData indexMetaData = INDEX_METADATA_FORMAT.loadLatestState(logger, namedXContentRegistry,
                nodeEnv.resolveIndexFolder(indexFolderName)).v1();
            if (indexMetaData != null) {
                metaDataBuilder.put(indexMetaData, false);
            } else {
                logger.debug("[{}] failed to find metadata for existing index location", indexFolderName);
            }
        }
        return metaDataBuilder.build();
    }

    MetaData loadAtomicFullState() throws IOException {
        final MetaState metaState = loadMetaState();
        if (metaState == null) {
            return MetaData.builder().build();
        }
        MetaData.Builder metaDataBuilder;
        MetaData globalMetaData = METADATA_FORMAT.loadGeneration(logger, namedXContentRegistry, metaState.getGlobalStateGeneration(),
            nodeEnv.nodeDataPaths());
        if (globalMetaData != null) {
            metaDataBuilder = MetaData.builder(globalMetaData);
        } else {
            // TODO: throw exception?
            metaDataBuilder = MetaData.builder();
        }

        for (Map.Entry<Index, Long> entry : metaState.getIndices().entrySet()) {
            final String indexFolderName = entry.getKey().getUUID();
            final IndexMetaData indexMetaData = INDEX_METADATA_FORMAT.loadGeneration(logger, namedXContentRegistry, entry.getValue(),
                nodeEnv.resolveIndexFolder(indexFolderName));
            if (indexMetaData != null) {
                metaDataBuilder.put(indexMetaData, false);
            } else {
                // TODO: throw exception?
                logger.debug("[{}] failed to find metadata for existing index location", indexFolderName);
            }
        }
        return metaDataBuilder.build();
    }

    /**
     * Loads the index state for the provided index name, returning null if doesn't exists.
     */
    public Tuple<IndexMetaData, Long> loadIndexState(Index index) throws IOException {
        return INDEX_METADATA_FORMAT.loadLatestState(logger, namedXContentRegistry, nodeEnv.indexPaths(index));
    }

    private static final ToXContent.Params INDEX_METADATA_FORMAT_PARAMS = new ToXContent.MapParams(Collections.singletonMap("binary", "true"));
    public static final String INDEX_STATE_FILE_PREFIX = "state-";

    /**
     * State format for {@link IndexMetaData} to write to and load from disk
     */
    public static final MetaDataStateFormat<IndexMetaData> INDEX_METADATA_FORMAT = new MetaDataStateFormat<IndexMetaData>(INDEX_STATE_FILE_PREFIX) {

        @Override
        public void toXContent(XContentBuilder builder, IndexMetaData state) throws IOException {
            IndexMetaData.Builder.toXContent(state, builder, INDEX_METADATA_FORMAT_PARAMS);
        }

        @Override
        public IndexMetaData fromXContent(XContentParser parser) throws IOException {
            assert parser.getXContentRegistry() != NamedXContentRegistry.EMPTY
                : "loading index metadata requires a working named xcontent registry";
            return IndexMetaData.Builder.fromXContent(parser);
        }
    };

    /**
     * Loads all indices states available on disk
     */
    List<IndexMetaData> loadIndicesStates(Predicate<String> excludeIndexPathIdsPredicate) throws IOException {
        List<IndexMetaData> indexMetaDataList = new ArrayList<>();
        for (String indexFolderName : nodeEnv.availableIndexFolders()) {
            if (excludeIndexPathIdsPredicate.test(indexFolderName)) {
                continue;
            }
            IndexMetaData indexMetaData = INDEX_METADATA_FORMAT.loadLatestState(logger, namedXContentRegistry,
                nodeEnv.resolveIndexFolder(indexFolderName)).v1();
            if (indexMetaData != null) {
                final String indexPathId = indexMetaData.getIndex().getUUID();
                if (indexFolderName.equals(indexPathId)) {
                    indexMetaDataList.add(indexMetaData);
                } else {
                    throw new IllegalStateException("[" + indexFolderName+ "] invalid index folder name, rename to [" + indexPathId + "]");
                }
            } else {
                logger.debug("[{}] failed to find metadata for existing index location", indexFolderName);
            }
        }
        return indexMetaDataList;
    }

    /**
     * Loads the global state, *without* index state, see {@link #loadFullState()} for that.
     */
    Tuple<MetaData, Long> loadGlobalState() throws IOException {
        return METADATA_FORMAT.loadLatestState(logger, namedXContentRegistry, nodeEnv.nodeDataPaths());
    }

    /**
     * Writes the index state.
     *
     * This method is public for testing purposes.
     */
    public long writeIndex(String reason, IndexMetaData indexMetaData) throws IOException {
        final Index index = indexMetaData.getIndex();
        logger.trace("[{}] writing state, reason [{}]", index, reason);
        try {
            final long generation = INDEX_METADATA_FORMAT.write(indexMetaData,
                nodeEnv.indexPaths(indexMetaData.getIndex()));
            logger.trace("[{}] state written (generation: {})", index, generation);
            return generation;
        } catch (Exception ex) {
            logger.warn(() -> new ParameterizedMessage("[{}]: failed to write index state", index), ex);
            throw new IOException("failed to write state for [" + index + "]", ex);
        }
    }

    /**
     * Writes the global state, *without* the indices states.
     */
    long writeGlobalState(String reason, MetaData metaData) throws IOException {
        logger.trace("[_global] writing state, reason [{}]",  reason);
        try {
            final long generation = METADATA_FORMAT.write(metaData, nodeEnv.nodeDataPaths());
            logger.trace("[_global] state written (generation: {})", generation);
            return generation;
        } catch (Exception ex) {
            logger.warn("[_global]: failed to write global state", ex);
            throw new IOException("failed to write global state", ex);
        }
    }

    private static final ToXContent.Params METADATA_FORMAT_PARAMS;
    static {
        Map<String, String> params = new HashMap<>(2);
        params.put("binary", "true");
        params.put(MetaData.CONTEXT_MODE_PARAM, MetaData.CONTEXT_MODE_GATEWAY);
        METADATA_FORMAT_PARAMS = new ToXContent.MapParams(params);
    }
    public static final String GLOBAL_STATE_FILE_PREFIX = "global-";

    /**
     * State format for {@link MetaData} to write to and load from disk
     */
    public static final MetaDataStateFormat<MetaData> METADATA_FORMAT = new MetaDataStateFormat<MetaData>(GLOBAL_STATE_FILE_PREFIX) {

        @Override
        public void toXContent(XContentBuilder builder, MetaData state) throws IOException {
            MetaData.Builder.toXContent(state, builder, METADATA_FORMAT_PARAMS);
        }

        @Override
        public MetaData fromXContent(XContentParser parser) throws IOException {
            return MetaData.Builder.fromXContent(parser);
        }
    };

    static class MetaState implements ToXContentFragment {

        public static final String META_STATE_FILE_PREFIX = "meta-";

        private final long globalStateGeneration;

        private final Map<Index, Long> indices;

        public MetaState(long globalStateGeneration, Map<Index, Long> indices) {
            this.globalStateGeneration = globalStateGeneration;
            this.indices = indices;
        }

        public long getGlobalStateGeneration() {
            return globalStateGeneration;
        }

        public Map<Index, Long> getIndices() {
            return indices;
        }

        private static final ParseField INDICES_PARSE_FIELD = new ParseField("indices");
        private static final ParseField INDEX_PARSE_FIELD = new ParseField("index");
        private static final ParseField GENERATION_PARSE_FIELD = new ParseField("generation");

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(GENERATION_PARSE_FIELD.getPreferredName(), globalStateGeneration);

            builder.startArray(INDICES_PARSE_FIELD.getPreferredName());
            for (Map.Entry<Index, Long> entry : indices.entrySet()) {
                builder.startObject();
                builder.field(INDEX_PARSE_FIELD.getPreferredName(), entry.getKey());
                builder.field(GENERATION_PARSE_FIELD.getPreferredName(), entry.getValue());
                builder.endObject();
            }
            builder.endArray();
            return builder;
        }

        private static final ConstructingObjectParser<MetaState, Void> PARSER = new ConstructingObjectParser<>("state",
            a -> new MetaState((Long) a[0], ((List<Tuple<Index, Long>>) a[1]).stream().collect(Collectors.toMap(Tuple::v1, Tuple::v2))));

        static {
            final ConstructingObjectParser<Tuple<Index, Long>, Void> INDEX_ENTRY_PARSER = new ConstructingObjectParser<>("index",
                a -> new Tuple(((Index.Builder) a[0]).build(), (Long) a[1]));

            INDEX_ENTRY_PARSER.declareObject(ConstructingObjectParser.constructorArg(), Index.INDEX_PARSER, INDEX_PARSE_FIELD);
            INDEX_ENTRY_PARSER.declareLong(ConstructingObjectParser.constructorArg(), GENERATION_PARSE_FIELD);

            PARSER.declareLong(ConstructingObjectParser.constructorArg(), GENERATION_PARSE_FIELD);
            PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), INDEX_ENTRY_PARSER, INDICES_PARSE_FIELD);
        }

        public static MetaState fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        private static final ToXContent.Params FORMAT_PARAMS = new MapParams(Collections.singletonMap("binary", "true"));

        public static final MetaDataStateFormat<MetaState> FORMAT = new MetaDataStateFormat<MetaState>(META_STATE_FILE_PREFIX) {

            @Override
            public void toXContent(XContentBuilder builder, MetaState state) throws IOException {
                state.toXContent(builder, FORMAT_PARAMS);
            }

            @Override
            public MetaState fromXContent(XContentParser parser) throws IOException {
                return MetaState.fromXContent(parser);
            }
        };

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MetaState metaState = (MetaState) o;
            return globalStateGeneration == metaState.globalStateGeneration &&
                Objects.equals(indices, metaState.indices);
        }

        @Override
        public int hashCode() {
            return Objects.hash(globalStateGeneration, indices);
        }
    }

    MetaState loadMetaState() throws IOException {
        return MetaState.FORMAT.loadLatestState(logger, namedXContentRegistry, nodeEnv.nodeDataPaths()).v1();
    }

    long writeMetaState(String reason, MetaState metaState) throws IOException {
        logger.trace("[_meta] writing state, reason [{}]",  reason);
        try {
            final long generation = MetaState.FORMAT.write(metaState, nodeEnv.nodeDataPaths());
            logger.trace("[_meta] state written (generation: {})", generation);
            return generation;
        } catch (Exception ex) {
            logger.warn("[_meta]: failed to write meta state", ex);
            throw new IOException("failed to write meta state", ex);
        }
    }
}
