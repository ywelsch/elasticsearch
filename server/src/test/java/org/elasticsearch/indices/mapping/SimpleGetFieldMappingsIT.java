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

package org.elasticsearch.indices.mapping;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.IndexMetaData.INDEX_METADATA_BLOCK;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_BLOCKS_METADATA;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_BLOCKS_READ;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_BLOCKS_WRITE;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_READ_ONLY;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertBlocked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class SimpleGetFieldMappingsIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(InternalSettingsPlugin.class);
    }

    public void testGetMappingsWhereThereAreNone() {
        createIndex("index");
        GetFieldMappingsResponse response = client().admin().indices().prepareGetFieldMappings().get();
        assertThat(response.mappings().size(), equalTo(1));
        assertThat(response.mappings().get("index").size(), equalTo(0));

        assertThat(response.fieldMappings("index", "type", "field"), Matchers.nullValue());
    }

    private XContentBuilder getMappingForType(String type) throws IOException {
        return jsonBuilder().startObject().startObject(type).startObject("properties")
                .startObject("field1").field("type", "text").endObject()
                .startObject("obj").startObject("properties").startObject("subfield").field("type", "keyword").endObject().endObject().endObject()
                .endObject().endObject().endObject();
    }

    public void testGetFieldMappings() throws Exception {

        assertAcked(prepareCreate("indexa")
            .addMapping("typeA", getMappingForType("typeA")));
        assertAcked(client().admin().indices().prepareCreate("indexb")
            .addMapping("typeB", getMappingForType("typeB")));


        // Get mappings by full name
        GetFieldMappingsResponse response = client().admin().indices().prepareGetFieldMappings("indexa").setTypes("typeA").setFields("field1", "obj.subfield").get();
        assertThat(response.fieldMappings("indexa", "typeA", "field1").fullName(), equalTo("field1"));
        assertThat(response.fieldMappings("indexa", "typeA", "field1").sourceAsMap(), hasKey("field1"));
        assertThat(response.fieldMappings("indexa", "typeA", "obj.subfield").fullName(), equalTo("obj.subfield"));
        assertThat(response.fieldMappings("indexa", "typeA", "obj.subfield").sourceAsMap(), hasKey("subfield"));
        assertThat(response.fieldMappings("indexb", "typeB", "field1"), nullValue());

        // Get mappings by name
        response = client().admin().indices().prepareGetFieldMappings("indexa").setTypes("typeA").setFields("field1", "obj.subfield").get();
        assertThat(response.fieldMappings("indexa", "typeA", "field1").fullName(), equalTo("field1"));
        assertThat(response.fieldMappings("indexa", "typeA", "field1").sourceAsMap(), hasKey("field1"));
        assertThat(response.fieldMappings("indexa", "typeA", "obj.subfield").fullName(), equalTo("obj.subfield"));
        assertThat(response.fieldMappings("indexa", "typeA", "obj.subfield").sourceAsMap(), hasKey("subfield"));
        assertThat(response.fieldMappings("indexa", "typeB", "field1"), nullValue());
        assertThat(response.fieldMappings("indexb", "typeB", "field1"), nullValue());

        // get mappings by name across multiple indices
        response = client().admin().indices().prepareGetFieldMappings().setTypes("typeA").setFields("obj.subfield").get();
        assertThat(response.fieldMappings("indexa", "typeA", "obj.subfield").fullName(), equalTo("obj.subfield"));
        assertThat(response.fieldMappings("indexa", "typeA", "obj.subfield").sourceAsMap(), hasKey("subfield"));
        assertThat(response.fieldMappings("indexa", "typeB", "obj.subfield"), nullValue());
        assertThat(response.fieldMappings("indexb", "typeB", "obj.subfield"), nullValue());

        // get mappings by name across multiple types
        response = client().admin().indices().prepareGetFieldMappings("indexa").setFields("obj.subfield").get();
        assertThat(response.fieldMappings("indexa", "typeA", "obj.subfield").fullName(), equalTo("obj.subfield"));
        assertThat(response.fieldMappings("indexa", "typeA", "obj.subfield").sourceAsMap(), hasKey("subfield"));
        assertThat(response.fieldMappings("indexa", "typeA", "field1"), nullValue());
        assertThat(response.fieldMappings("indexb", "typeB", "obj.subfield"), nullValue());
        assertThat(response.fieldMappings("indexb", "typeB", "field1"), nullValue());

        // get mappings by name across multiple types & indices
        response = client().admin().indices().prepareGetFieldMappings().setFields("obj.subfield").get();
        assertThat(response.fieldMappings("indexa", "typeA", "obj.subfield").fullName(), equalTo("obj.subfield"));
        assertThat(response.fieldMappings("indexa", "typeA", "obj.subfield").sourceAsMap(), hasKey("subfield"));
        assertThat(response.fieldMappings("indexa", "typeA", "field1"), nullValue());
        assertThat(response.fieldMappings("indexb", "typeB", "field1"), nullValue());
        assertThat(response.fieldMappings("indexb", "typeB", "obj.subfield").fullName(), equalTo("obj.subfield"));
        assertThat(response.fieldMappings("indexb", "typeB", "obj.subfield").sourceAsMap(), hasKey("subfield"));
        assertThat(response.fieldMappings("indexb", "typeB", "field1"), nullValue());
    }

    @SuppressWarnings("unchecked")
    public void testSimpleGetFieldMappingsWithDefaults() throws Exception {
        assertAcked(prepareCreate("test").addMapping("type", getMappingForType("type")));

        client().prepareIndex("test", "type", "1").setSource("num", 1).get();

        GetFieldMappingsResponse response = client().admin().indices().prepareGetFieldMappings().setFields("num", "field1", "obj.subfield").includeDefaults(true).get();

        assertThat((Map<String, Object>) response.fieldMappings("test", "type", "num").sourceAsMap().get("num"), hasEntry("index", Boolean.TRUE));
        assertThat((Map<String, Object>) response.fieldMappings("test", "type", "num").sourceAsMap().get("num"), hasEntry("type", (Object) "long"));
        assertThat((Map<String, Object>) response.fieldMappings("test", "type", "field1").sourceAsMap().get("field1"), hasEntry("index", Boolean.TRUE));
        assertThat((Map<String, Object>) response.fieldMappings("test", "type", "field1").sourceAsMap().get("field1"), hasEntry("type", (Object) "text"));
        assertThat((Map<String, Object>) response.fieldMappings("test", "type", "obj.subfield").sourceAsMap().get("subfield"), hasEntry("type", (Object) "keyword"));


    }

    //fix #6552
    public void testSimpleGetFieldMappingsWithPretty() throws Exception {
        assertAcked(prepareCreate("index").addMapping("type", getMappingForType("type")));
        Map<String, String> params = new HashMap<>();
        params.put("pretty", "true");
        GetFieldMappingsResponse response = client().admin().indices().prepareGetFieldMappings("index").setTypes("type").setFields("field1", "obj.subfield").get();
        XContentBuilder responseBuilder = XContentFactory.jsonBuilder().prettyPrint();
        responseBuilder.startObject();
        response.toXContent(responseBuilder, new ToXContent.MapParams(params));
        responseBuilder.endObject();
        String responseStrings = Strings.toString(responseBuilder);


        XContentBuilder prettyJsonBuilder = XContentFactory.jsonBuilder().prettyPrint();
        prettyJsonBuilder.copyCurrentStructure(createParser(JsonXContent.jsonXContent, responseStrings));
        assertThat(responseStrings, equalTo(Strings.toString(prettyJsonBuilder)));

        params.put("pretty", "false");

        response = client().admin().indices().prepareGetFieldMappings("index").setTypes("type").setFields("field1", "obj.subfield").get();
        responseBuilder = XContentFactory.jsonBuilder().prettyPrint().lfAtEnd();
        responseBuilder.startObject();
        response.toXContent(responseBuilder, new ToXContent.MapParams(params));
        responseBuilder.endObject();
        responseStrings = Strings.toString(responseBuilder);

        prettyJsonBuilder = XContentFactory.jsonBuilder().prettyPrint();
        prettyJsonBuilder.copyCurrentStructure(createParser(JsonXContent.jsonXContent, responseStrings));
        assertThat(responseStrings, not(equalTo(Strings.toString(prettyJsonBuilder))));

    }

    public void testGetFieldMappingsWithBlocks() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("_doc", getMappingForType("_doc")));

        for (String block : Arrays.asList(SETTING_BLOCKS_READ, SETTING_BLOCKS_WRITE, SETTING_READ_ONLY)) {
            try {
                enableIndexBlock("test", block);
                GetFieldMappingsResponse response = client().admin().indices().prepareGetFieldMappings("test").setTypes("_doc")
                    .setFields("field1", "obj.subfield").get();
                assertThat(response.fieldMappings("test", "_doc", "field1").fullName(), equalTo("field1"));
            } finally {
                disableIndexBlock("test", block);
            }
        }

        try {
            enableIndexBlock("test", SETTING_BLOCKS_METADATA);
            assertBlocked(client().admin().indices().prepareGetMappings(), INDEX_METADATA_BLOCK);
        } finally {
            disableIndexBlock("test", SETTING_BLOCKS_METADATA);
        }
    }
}
