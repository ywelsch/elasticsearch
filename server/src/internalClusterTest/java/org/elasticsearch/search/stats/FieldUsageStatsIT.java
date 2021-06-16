/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.stats;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.search.stats.FieldUsageStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.internal.FieldUsageTrackingDirectoryReader.UsageContext;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.Set;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAllSuccessful;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

public class FieldUsageStatsIT extends ESSingleNodeTestCase {

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put("search.aggs.rewrite_to_filter_by_filter", false).build();
    }

    public void testFieldUsageStats() {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(Settings.builder()
            .put(SETTING_NUMBER_OF_SHARDS, 1)
            .put(SETTING_NUMBER_OF_REPLICAS, 0)));
        IndexShard indexShard = null;
        for (IndexService indexService : getInstanceFromNode(IndicesService.class)) {
            if (indexService.index().getName().equals("test")) {
                indexShard = indexService.getShard(0);
                break;
            }
        }

        assertNotNull(indexShard);
        int docsTest1 = scaledRandomIntBetween(1, 5);
        for (int i = 0; i < docsTest1; i++) {
            client().prepareIndex("test").setId(Integer.toString(i)).setSource(
                "field", "value", "field2", "value2").get();
        }
        client().admin().indices().prepareRefresh("test").get();

        final FieldUsageStats stats = indexShard.fieldUsageStats();

        assertFalse(stats.getPerFieldStats().containsKey("field"));
        assertFalse(stats.getPerFieldStats().containsKey("field.keyword"));
        assertFalse(stats.getPerFieldStats().containsKey("field2"));

        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(QueryBuilders.termQuery("field", "value"))
            .addAggregation(AggregationBuilders.terms("agg1").field("field.keyword"))
            .addAggregation(AggregationBuilders.filter("agg2", QueryBuilders.spanTermQuery("field2", "value2")))
            .setSize(100)
            .get();
        assertHitCount(searchResponse, docsTest1);
        assertAllSuccessful(searchResponse);

        logger.info("Stats after first query: {}", stats.getPerFieldStats());

        assertTrue(stats.getPerFieldStats().containsKey("_id"));
        assertEquals(Set.of(UsageContext.STORED_FIELDS), stats.getPerFieldStats().get("_id").keySet());
        assertTrue(stats.getPerFieldStats().containsKey("_source"));
        assertEquals(Set.of(UsageContext.STORED_FIELDS), stats.getPerFieldStats().get("_source").keySet());

        assertTrue(stats.getPerFieldStats().containsKey("field"));
        // we sort by _score
        assertEquals(Set.of(UsageContext.TERMS, UsageContext.FREQS, UsageContext.NORMS), stats.getPerFieldStats().get("field").keySet());
        assertEquals(1L, stats.getPerFieldStats().get("field").get(UsageContext.TERMS));

        assertTrue(stats.getPerFieldStats().containsKey("field2"));
        // positions because of span query
        assertEquals(Set.of(UsageContext.TERMS, UsageContext.FREQS, UsageContext.POSITIONS),
            stats.getPerFieldStats().get("field2").keySet());
        assertEquals(1L, stats.getPerFieldStats().get("field2").get(UsageContext.TERMS));

        assertTrue(stats.getPerFieldStats().containsKey("field.keyword"));
        // terms agg does not use search as we've set search.aggs.rewrite_to_filter_by_filter to false
        assertEquals(Set.of(UsageContext.DOC_VALUES), stats.getPerFieldStats().get("field.keyword").keySet());
        assertEquals(1L, stats.getPerFieldStats().get("field.keyword").get(UsageContext.DOC_VALUES));

        client().prepareSearch()
            .setQuery(QueryBuilders.termQuery("field", "value"))
            .addAggregation(AggregationBuilders.terms("agg1").field("field.keyword"))
            .setSize(100)
            .get();

        logger.info("Stats after second query: {}", stats.getPerFieldStats());

        assertEquals(2L, stats.getPerFieldStats().get("field").get(UsageContext.TERMS));
        assertEquals(1L, stats.getPerFieldStats().get("field2").get(UsageContext.TERMS));
        assertEquals(2L, stats.getPerFieldStats().get("field.keyword").get(UsageContext.DOC_VALUES));
    }
}
