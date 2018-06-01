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

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;

@TestLogging("_root:DEBUG")
public class Zen2IT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(Zen2Plugin.class);
    }

    @Override
    protected boolean addTestZenDiscovery() {
        return false;
    }

    public void testCluster() throws Exception {
        String[] indexNames = { "test1", "test2" };
        for (String indexName : indexNames) {
            assertAcked(client()
                .admin()
                .indices()
                .prepareCreate(indexName));

            indexRandom(true, client().prepareIndex(indexName, "type1", indexName + "1").setSource("field1", "value1"));
        }

        for (String indexName : indexNames) {
            SearchResponse request = client().prepareSearch().setQuery(constantScoreQuery(termQuery("_index", indexName))).get();
            SearchResponse searchResponse = assertSearchResponse(request);
            assertHitCount(searchResponse, 1L);
            assertSearchHits(searchResponse, indexName + "1");
        }

        internalCluster().startNode();

        internalCluster().stopCurrentMasterNode();

        ensureClusterSizeConsistency();

        createIndex("test3");

        logger.info("yay");
    }

}
