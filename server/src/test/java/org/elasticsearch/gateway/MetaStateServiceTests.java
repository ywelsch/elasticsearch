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

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class MetaStateServiceTests extends ESTestCase {
    private static Settings indexSettings = Settings.builder()
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .build();

    public void testWriteLoadIndex() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(Settings.EMPTY, env, xContentRegistry());

            IndexMetaData index = IndexMetaData.builder("test1").settings(indexSettings).build();
            metaStateService.writeIndex("test_write", index);
            assertThat(metaStateService.loadIndexState(index.getIndex()).v1(), equalTo(index));
        }
    }

    public void testLoadMissingIndex() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(Settings.EMPTY, env, xContentRegistry());
            assertThat(metaStateService.loadIndexState(new Index("test1", "test1UUID")).v1(), nullValue());
        }
    }

    public void testWriteLoadGlobal() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(Settings.EMPTY, env, xContentRegistry());

            MetaData metaData = MetaData.builder()
                    .persistentSettings(Settings.builder().put("test1", "value1").build())
                    .build();
            metaStateService.writeGlobalState("test_write", metaData);
            assertThat(metaStateService.loadGlobalState().v1().persistentSettings(), equalTo(metaData.persistentSettings()));
        }
    }

    public void testWriteLoadMeta() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(Settings.EMPTY, env, xContentRegistry());

            final Map<Index,Long> indices = new HashMap<>();
            for (int i = 0; i < randomIntBetween(0, 10); i++) {
                indices.put(new Index(randomAlphaOfLength(10), randomAlphaOfLength(10)), randomLong());
            }
            final MetaStateService.MetaState metaState = new MetaStateService.MetaState(randomLong(), indices);
            metaStateService.writeMetaState("test_write", metaState);
            assertThat(metaState, equalTo(metaStateService.loadMetaState()));
        }
    }

    public void testWriteGlobalStateWithIndexAndNoIndexIsLoaded() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(Settings.EMPTY, env, xContentRegistry());

            MetaData metaData = MetaData.builder()
                    .persistentSettings(Settings.builder().put("test1", "value1").build())
                    .build();
            IndexMetaData index = IndexMetaData.builder("test1").settings(indexSettings).build();
            MetaData metaDataWithIndex = MetaData.builder(metaData).put(index, true).build();

            metaStateService.writeGlobalState("test_write", metaDataWithIndex);
            assertThat(metaStateService.loadGlobalState().v1().persistentSettings(), equalTo(metaData.persistentSettings()));
            assertThat(metaStateService.loadGlobalState().v1().hasIndex("test1"), equalTo(false));
        }
    }

    public void testLoadGlobal() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(Settings.EMPTY, env, xContentRegistry());

            IndexMetaData index = IndexMetaData.builder("test1").settings(indexSettings).build();
            MetaData metaData = MetaData.builder()
                    .persistentSettings(Settings.builder().put("test1", "value1").build())
                    .put(index, true)
                    .build();

            metaStateService.writeGlobalState("test_write", metaData);
            metaStateService.writeIndex("test_write", index);

            MetaData loadedState = metaStateService.loadFullState();
            assertThat(loadedState.persistentSettings(), equalTo(metaData.persistentSettings()));
            assertThat(loadedState.hasIndex("test1"), equalTo(true));
            assertThat(loadedState.index("test1"), equalTo(index));
        }
    }
}
