package org.elasticsearch.snapshots;

import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MockEngineFactoryPlugin;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.slice.SliceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

@ESIntegTestCase.ClusterScope(numDataNodes = 0)
public class SearchableSnapshotIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        Collection<Class<? extends Plugin>> classes = new ArrayList<>(super.nodePlugins());
        classes.add(SearchableSnapshotIT.MyPlugin.class);
        return classes;
    }

    @Override
    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        Collection<Class<? extends Plugin>> classes = new ArrayList<>(super.getMockPlugins());
        classes.remove(MockEngineFactoryPlugin.class);
        return classes;
    }

    public static final class MyPlugin extends Plugin implements RepositoryPlugin, EnginePlugin {
        @Override
        public Map<String, Repository.Factory> getRepositories(Environment env, NamedXContentRegistry namedXContentRegistry,
                                                               ThreadPool threadPool) {
            return Collections.singletonMap("searchable", SearchableSnapshotRepository.newRepositoryFactory());
        }
        @Override
        public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
            if (indexSettings.getValue(SearchableSnapshotRepository.SEARCHABLE_SNAPSHOTS)) {
                return Optional.of(SearchableSnapshotRepository.getEngineFactory());
            }
            return Optional.empty();
        }

        @Override
        public List<Setting<?>> getSettings() {
            List<Setting<?>> settings = new ArrayList<>(super.getSettings());
            settings.add(SearchableSnapshotRepository.SEARCHABLE_SNAPSHOTS);
            settings.add(SearchableSnapshotRepository.SEARCHABLE_SNAPSHOTS_REPOSITORY_NAME);
            settings.add(SearchableSnapshotRepository.SEARCHABLE_SNAPSHOTS_SNAPSHOT_NAME);
            settings.add(SearchableSnapshotRepository.SEARCHABLE_SNAPSHOTS_SNAPSHOT_UUID);
            settings.add(SearchableSnapshotRepository.SEARCHABLE_SNAPSHOTS_INDEX_NAME);
            settings.add(SearchableSnapshotRepository.SEARCHABLE_SNAPSHOTS_INDEX_ID);
            return settings;
        }
    }

    public void testSnapshotAndRestore() throws Exception {
        final String sourceIdx = "test-idx";
        IndexRequestBuilder[] builders = snashotAndRestore(sourceIdx, randomIntBetween(1, 5));
        IndicesStatsResponse indicesStatsResponse = client().admin().indices().prepareStats(sourceIdx).clear().setDocs(true).get();
        assertEquals(builders.length, indicesStatsResponse.getTotal().getDocs().getCount());
        assertHits(sourceIdx, builders.length);

        internalCluster().ensureAtLeastNumDataNodes(2);
//        client().admin().indices().prepareUpdateSettings(sourceIdx)
//            .setSettings(Settings.builder().put("index.number_of_replicas", 1)).get();
        ensureGreen(sourceIdx);
        assertHits(sourceIdx, builders.length);
    }

    private void assertHits(String index, int numDocsExpected) {
        SearchResponse searchResponse = client().prepareSearch(index).setIndicesOptions(IndicesOptions.STRICT_EXPAND_OPEN_FORBID_CLOSED)
            .addSort(SeqNoFieldMapper.NAME, SortOrder.ASC)
            .setSize(numDocsExpected).get();
        Consumer<SearchResponse> assertConsumer = res -> {
            SearchHits hits = res.getHits();
            for (SearchHit hit : hits) {
                String id = hit.getId();
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                assertTrue(sourceAsMap.containsKey("field1"));
                assertEquals("bar " + id, sourceAsMap.get("field1"));
                assertEquals("r" + id, hit.field("_routing").getValue());
            }
        };
        assertConsumer.accept(searchResponse);
        assertEquals(numDocsExpected, searchResponse.getHits().getTotalHits().value);
        searchResponse = client().prepareSearch(index).setIndicesOptions(IndicesOptions.STRICT_EXPAND_OPEN_FORBID_CLOSED)
            .addSort(SeqNoFieldMapper.NAME, SortOrder.ASC)
            .setScroll("1m")
            .slice(new SliceBuilder(SeqNoFieldMapper.NAME, randomIntBetween(0,1), 2))
            .setSize(randomIntBetween(1, 10)).get();
        try {
            do {
                // now do a scroll with a slice
                assertConsumer.accept(searchResponse);
                searchResponse = client().prepareSearchScroll(searchResponse.getScrollId()).setScroll(TimeValue.timeValueMinutes(1)).get();
            } while (searchResponse.getHits().getHits().length > 0);
        } finally {
            if (searchResponse.getScrollId() != null) {
                client().prepareClearScroll().addScrollId(searchResponse.getScrollId()).get();
            }
        }
    }

    private IndexRequestBuilder[] snashotAndRestore(final String sourceIdx,
                                                    final int numShards) throws InterruptedException, IOException {
        logger.info("-->  starting a master node and a data node");
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();

        final Client client = client();
        final String repo = "test-repo";
        final String snapshot = "test-snap";

        logger.info("-->  creating repository");
        assertAcked(client.admin().cluster().preparePutRepository(repo).setType("searchable")
            .setSettings(Settings.builder().put("location", randomRepoPath())
                .put("delegate_type", "fs")
                .put("compress", randomBoolean())));

        CreateIndexRequestBuilder createIndexRequestBuilder = prepareCreate(sourceIdx, 0, Settings.builder()
            .put("number_of_shards", numShards).put("number_of_replicas", 0));
        List<Object> mappings = new ArrayList<>();
        if (mappings.isEmpty() == false) {
            createIndexRequestBuilder.addMapping("_doc", mappings.toArray());
        }
        assertAcked(createIndexRequestBuilder);
        ensureGreen();

        logger.info("--> indexing some data");
        IndexRequestBuilder[] builders = new IndexRequestBuilder[randomIntBetween(10, 100)];
        for (int i = 0; i < builders.length; i++) {
            XContentBuilder source = jsonBuilder()
                .startObject()
                .field("field1", "bar " + i);
            source.endObject();
            builders[i] = client().prepareIndex(sourceIdx, "_doc",
                Integer.toString(i)).setSource(source).setRouting("r" + i);
        }
        indexRandom(true, false, builders);
        flushAndRefresh();
        assertHitCount(client().prepareSearch(sourceIdx).setQuery(QueryBuilders.idsQuery().addIds("0")).get(), 1);

        logger.info("--> snapshot the index");
        CreateSnapshotResponse createResponse = client.admin().cluster()
            .prepareCreateSnapshot(repo, snapshot)
            .setWaitForCompletion(true).setIndices(sourceIdx).get();
        assertEquals(SnapshotState.SUCCESS, createResponse.getSnapshotInfo().state());

        logger.info("--> delete index and stop the data node");
        assertAcked(client.admin().indices().prepareDelete(sourceIdx).get());
        internalCluster().stopRandomDataNode();
        client().admin().cluster().prepareHealth().setTimeout("30s").setWaitForNodes("1");

        final String newDataNode = internalCluster().startDataOnlyNode();
        logger.info("--> start a new data node " + newDataNode);
        client().admin().cluster().prepareHealth().setTimeout("30s").setWaitForNodes("2");

        logger.info("--> restore the index and ensure all shards are allocated");
        RestoreSnapshotResponse restoreResponse = client().admin().cluster()
            .prepareRestoreSnapshot(repo, snapshot).setWaitForCompletion(true)
            .setIndices(sourceIdx).get();
        assertEquals(restoreResponse.getRestoreInfo().totalShards(),
            restoreResponse.getRestoreInfo().successfulShards());
        ensureYellow();
        return builders;
    }
}
