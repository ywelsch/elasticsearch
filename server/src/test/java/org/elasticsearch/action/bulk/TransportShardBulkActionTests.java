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

package org.elasticsearch.action.bulk;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.Version;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.TransportShardBulkAction.ReplicaItemExecutionMode;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.support.replication.TransportWriteAction.WritePrimaryResult;
import org.elasticsearch.action.update.UpdateHelper;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.rest.RestStatus;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.action.bulk.TransportShardBulkAction.replicaItemExecutionMode;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportShardBulkActionTests extends IndexShardTestCase {

    private final ShardId shardId = new ShardId("index", "_na_", 0);
    private final Settings idxSettings = Settings.builder()
        .put("index.number_of_shards", 1)
        .put("index.number_of_replicas", 0)
        .put("index.version.created", Version.CURRENT.id)
        .build();

    private IndexMetaData indexMetaData() throws IOException {
        return IndexMetaData.builder("index")
            .putMapping("_doc",
                "{\"properties\":{\"foo\":{\"type\":\"text\",\"fields\":" +
                    "{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}}}}")
            .settings(idxSettings)
            .primaryTerm(0, 1).build();
    }

    private ClusterService clusterService;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        clusterService = createClusterService(threadPool);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.close();
    }

    public void testShouldExecuteReplicaItem() throws Exception {
        // Successful index request should be replicated
        DocWriteRequest<IndexRequest> writeRequest = new IndexRequest("index", "_doc", "id")
            .source(Requests.INDEX_CONTENT_TYPE, "foo", "bar");
        DocWriteResponse response = new IndexResponse(shardId, "type", "id", 1, 17, 1, randomBoolean());
        BulkItemRequest request = new BulkItemRequest(0, writeRequest);
        request.setPrimaryResponse(new BulkItemResponse(0, DocWriteRequest.OpType.INDEX, response));
        assertThat(replicaItemExecutionMode(request, 0),
            equalTo(ReplicaItemExecutionMode.NORMAL));

        // Failed index requests without sequence no should not be replicated
        writeRequest = new IndexRequest("index", "_doc", "id")
            .source(Requests.INDEX_CONTENT_TYPE, "foo", "bar");
        request = new BulkItemRequest(0, writeRequest);
        request.setPrimaryResponse(
            new BulkItemResponse(0, DocWriteRequest.OpType.INDEX,
                new BulkItemResponse.Failure("index", "type", "id",
                    new IllegalArgumentException("i died"))));
        assertThat(replicaItemExecutionMode(request, 0),
            equalTo(ReplicaItemExecutionMode.NOOP));

        // Failed index requests with sequence no should be replicated
        request = new BulkItemRequest(0, writeRequest);
        request.setPrimaryResponse(
            new BulkItemResponse(0, DocWriteRequest.OpType.INDEX,
                new BulkItemResponse.Failure("index", "type", "id",
                    new IllegalArgumentException(
                        "i died after sequence no was generated"),
                    1)));
        assertThat(replicaItemExecutionMode(request, 0),
            equalTo(ReplicaItemExecutionMode.FAILURE));
        // NOOP requests should not be replicated
        DocWriteRequest<UpdateRequest> updateRequest = new UpdateRequest("index", "type", "id");
        response = new UpdateResponse(shardId, "type", "id", 1, DocWriteResponse.Result.NOOP);
        request = new BulkItemRequest(0, updateRequest);
        request.setPrimaryResponse(new BulkItemResponse(0, DocWriteRequest.OpType.UPDATE,
            response));
        assertThat(replicaItemExecutionMode(request, 0),
            equalTo(ReplicaItemExecutionMode.NOOP));
    }

    public void testExecuteBulkIndexRequest() throws Exception {
        IndexShard shard = newStartedShard(true);

        BulkItemRequest[] items = new BulkItemRequest[1];
        boolean create = randomBoolean();
        DocWriteRequest<IndexRequest> writeRequest = new IndexRequest("index", "_doc", "id").source(Requests.INDEX_CONTENT_TYPE)
            .create(create);
        BulkItemRequest primaryRequest = new BulkItemRequest(0, writeRequest);
        items[0] = primaryRequest;
        BulkShardRequest bulkShardRequest =
            new BulkShardRequest(shardId, RefreshPolicy.NONE, items);

        UpdateHelper updateHelper = null;
        PrimaryExecutionContext context = new PrimaryExecutionContext(bulkShardRequest, shard);
        context.advance();
        TransportShardBulkAction.executeBulkItemRequest(context, updateHelper, threadPool::absoluteTimeInMillis,
            new NoopMappingUpdatePerformer());

        // Translog should change, since there were no problems
        assertNotNull(context.getLocationToSync());

        BulkItemResponse primaryResponse = bulkShardRequest.items()[0].getPrimaryResponse();

        assertThat(primaryResponse.getItemId(), equalTo(0));
        assertThat(primaryResponse.getId(), equalTo("id"));
        assertThat(primaryResponse.getOpType(),
            equalTo(create ? DocWriteRequest.OpType.CREATE : DocWriteRequest.OpType.INDEX));
        assertFalse(primaryResponse.isFailed());

        // Assert that the document actually made it there
        assertDocCount(shard, 1);

        writeRequest = new IndexRequest("index", "_doc", "id").source(Requests.INDEX_CONTENT_TYPE).create(true);
        primaryRequest = new BulkItemRequest(0, writeRequest);
        items[0] = primaryRequest;
        bulkShardRequest = new BulkShardRequest(shardId, RefreshPolicy.NONE, items);

        PrimaryExecutionContext secondContext = new PrimaryExecutionContext(bulkShardRequest, shard);
        secondContext.advance();
        TransportShardBulkAction.executeBulkItemRequest(secondContext, updateHelper,
            threadPool::absoluteTimeInMillis, new ThrowingMappingUpdatePerformer(new RuntimeException("fail")));

        assertNull(secondContext.getLocationToSync());

        BulkItemRequest replicaRequest = bulkShardRequest.items()[0];

        primaryResponse = bulkShardRequest.items()[0].getPrimaryResponse();

        assertThat(primaryResponse.getItemId(), equalTo(0));
        assertThat(primaryResponse.getId(), equalTo("id"));
        assertThat(primaryResponse.getOpType(), equalTo(DocWriteRequest.OpType.CREATE));
        // Should be failed since the document already exists
        assertTrue(primaryResponse.isFailed());

        BulkItemResponse.Failure failure = primaryResponse.getFailure();
        assertThat(failure.getIndex(), equalTo("index"));
        assertThat(failure.getType(), equalTo("_doc"));
        assertThat(failure.getId(), equalTo("id"));
        assertThat(failure.getCause().getClass(), equalTo(VersionConflictEngineException.class));
        assertThat(failure.getCause().getMessage(),
            containsString("version conflict, document already exists (current version [1])"));
        assertThat(failure.getStatus(), equalTo(RestStatus.CONFLICT));

        assertThat(replicaRequest, equalTo(primaryRequest));

        // Assert that the document count is still 1
        assertDocCount(shard, 1);
        closeShards(shard);
    }

    public void testSkipBulkIndexRequestIfAborted() throws Exception {
        IndexShard shard = newStartedShard(true);

        BulkItemRequest[] items = new BulkItemRequest[randomIntBetween(2, 5)];
        for (int i = 0; i < items.length; i++) {
            DocWriteRequest<IndexRequest> writeRequest = new IndexRequest("index", "_doc", "id_" + i)
                .source(Requests.INDEX_CONTENT_TYPE)
                .opType(DocWriteRequest.OpType.INDEX);
            items[i] = new BulkItemRequest(i, writeRequest);
        }
        BulkShardRequest bulkShardRequest = new BulkShardRequest(shardId, RefreshPolicy.NONE, items);

        // Preemptively abort one of the bulk items, but allow the others to proceed
        BulkItemRequest rejectItem = randomFrom(items);
        RestStatus rejectionStatus = randomFrom(RestStatus.BAD_REQUEST, RestStatus.CONFLICT, RestStatus.FORBIDDEN, RestStatus.LOCKED);
        final ElasticsearchStatusException rejectionCause = new ElasticsearchStatusException("testing rejection", rejectionStatus);
        rejectItem.abort("index", rejectionCause);

        UpdateHelper updateHelper = null;
        WritePrimaryResult<BulkShardRequest, BulkShardResponse> result = TransportShardBulkAction.performOnPrimary(
            bulkShardRequest, shard, updateHelper, threadPool::absoluteTimeInMillis, new NoopMappingUpdatePerformer(),
            new ClusterStateObserver(clusterService, logger, threadPool.getThreadContext()), clusterService.localNode());

        // since at least 1 item passed, the tran log location should exist,
        assertThat(result.location, notNullValue());
        // and the response should exist and match the item count
        assertThat(result.finalResponseIfSuccessful, notNullValue());
        assertThat(result.finalResponseIfSuccessful.getResponses(), arrayWithSize(items.length));

        // check each response matches the input item, including the rejection
        for (int i = 0; i < items.length; i++) {
            BulkItemResponse response = result.finalResponseIfSuccessful.getResponses()[i];
            assertThat(response.getItemId(), equalTo(i));
            assertThat(response.getIndex(), equalTo("index"));
            assertThat(response.getType(), equalTo("_doc"));
            assertThat(response.getId(), equalTo("id_" + i));
            assertThat(response.getOpType(), equalTo(DocWriteRequest.OpType.INDEX));
            if (response.getItemId() == rejectItem.id()) {
                assertTrue(response.isFailed());
                assertThat(response.getFailure().getCause(), equalTo(rejectionCause));
                assertThat(response.status(), equalTo(rejectionStatus));
            } else {
                assertFalse(response.isFailed());
            }
        }

        // Check that the non-rejected updates made it to the shard
        assertDocCount(shard, items.length - 1);
        closeShards(shard);
    }

    public void testExecuteBulkIndexRequestWithMappingUpdates() throws Exception {
        IndexShard shard = newStartedShard(true);

        BulkItemRequest[] items = new BulkItemRequest[3];
        DocWriteRequest<IndexRequest> writeRequest = new IndexRequest("index", "_doc", "id")
            .source(Requests.INDEX_CONTENT_TYPE, "foo", "bar");
        items[0] = new BulkItemRequest(0, writeRequest);
        BulkShardRequest bulkShardRequest =
            new BulkShardRequest(shardId, RefreshPolicy.NONE, items);


        // Pretend the mappings haven't made it to the node yet
        PrimaryExecutionContext context = new PrimaryExecutionContext(bulkShardRequest, shard);
        context.advance();
        AtomicInteger updateCalled = new AtomicInteger();
        TransportShardBulkAction.executeBulkItemRequest(context, null, threadPool::absoluteTimeInMillis,
            (update, shardId, type) -> {
                // There should indeed be a mapping update
                assertNotNull(update);
                updateCalled.incrementAndGet();
            });
        assertTrue(context.requiresWaitingForMappingUpdate());

        assertThat("mappings were \"updated\" once", updateCalled.get(), equalTo(1));

        // Verify that the shard "executed" the operation twice
        verify(shard, times(2)).applyIndexOperationOnPrimary(anyLong(), any(), any(), anyLong(), anyBoolean());

        final MapperService mapperService = shard.mapperService();
        logger.info("--> mapperService.index(): {}", mapperService.index());
        mapperService.updateMapping(indexMetaData());
        context.resetForExecutionAfterRetry();

        TransportShardBulkAction.executeIndexRequestOnPrimary(context,
            (update, shardId, type) -> fail("should not have had to update the mappings"));


        // Verify that the shard "executed" the operation only once (2 for previous invocations plus
        // 1 for this execution)
        verify(shard, times(3)).applyIndexOperationOnPrimary(anyLong(), any(), any(), anyLong(), anyBoolean());


        BulkItemResponse primaryResponse = bulkShardRequest.items()[0].getPrimaryResponse();

        assertThat(primaryResponse.getItemId(), equalTo(0));
        assertThat(primaryResponse.getId(), equalTo("id"));
        assertThat(primaryResponse.getOpType(), equalTo(DocWriteRequest.OpType.CREATE));
        assertFalse(primaryResponse.isFailed());

        closeShards(shard);
    }

    public void testExecuteBulkIndexRequestWithErrorWhileUpdatingMapping() throws Exception {
        IndexShard shard = newStartedShard(true);

        BulkItemRequest[] items = new BulkItemRequest[1];
        DocWriteRequest<IndexRequest> writeRequest = new IndexRequest("index", "_doc", "id")
            .source(Requests.INDEX_CONTENT_TYPE, "foo", "bar");
        items[0] = new BulkItemRequest(0, writeRequest);
        BulkShardRequest bulkShardRequest =
            new BulkShardRequest(shardId, RefreshPolicy.NONE, items);

        UpdateHelper updateHelper = null;

        // Return an exception when trying to update the mapping
        RuntimeException err = new RuntimeException("some kind of exception");

        PrimaryExecutionContext context = new PrimaryExecutionContext(bulkShardRequest, shard);
        context.advance();
        TransportShardBulkAction.executeBulkItemRequest(context, updateHelper, threadPool::absoluteTimeInMillis,
            new ThrowingMappingUpdatePerformer(err));

        // Translog shouldn't be synced, as there were conflicting mappings
        assertThat(context.getLocationToSync(), nullValue());

        BulkItemResponse primaryResponse = bulkShardRequest.items()[0].getPrimaryResponse();

        // Since this was not a conflict failure, the primary response
        // should be filled out with the failure information
        assertThat(primaryResponse.getItemId(), equalTo(0));
        assertThat(primaryResponse.getId(), equalTo("id"));
        assertThat(primaryResponse.getOpType(), equalTo(DocWriteRequest.OpType.INDEX));
        assertTrue(primaryResponse.isFailed());
        assertThat(primaryResponse.getFailureMessage(), containsString("some kind of exception"));
        BulkItemResponse.Failure failure = primaryResponse.getFailure();
        assertThat(failure.getIndex(), equalTo("index"));
        assertThat(failure.getType(), equalTo("_doc"));
        assertThat(failure.getId(), equalTo("id"));
        assertThat(failure.getCause(), equalTo(err));

        closeShards(shard);
    }

    public void testExecuteBulkDeleteRequest() throws Exception {
        IndexShard shard = newStartedShard(true);

        BulkItemRequest[] items = new BulkItemRequest[1];
        DocWriteRequest<DeleteRequest> writeRequest = new DeleteRequest("index", "_doc", "id");
        items[0] = new BulkItemRequest(0, writeRequest);
        BulkShardRequest bulkShardRequest =
            new BulkShardRequest(shardId, RefreshPolicy.NONE, items);

        Translog.Location location = new Translog.Location(0, 0, 0);
        UpdateHelper updateHelper = null;

        PrimaryExecutionContext context = new PrimaryExecutionContext(bulkShardRequest, shard);
        context.advance();
        TransportShardBulkAction.executeBulkItemRequest(context, updateHelper, threadPool::absoluteTimeInMillis,
            new NoopMappingUpdatePerformer());

        // Translog changes, even though the document didn't exist
        assertThat(context.getLocationToSync(), not(location));

        BulkItemRequest replicaRequest = bulkShardRequest.items()[0];
        DocWriteRequest<?> replicaDeleteRequest = replicaRequest.request();
        BulkItemResponse primaryResponse = replicaRequest.getPrimaryResponse();
        DeleteResponse response = primaryResponse.getResponse();

        // Any version can be matched on replica
        assertThat(replicaDeleteRequest.version(), equalTo(Versions.MATCH_ANY));
        assertThat(replicaDeleteRequest.versionType(), equalTo(VersionType.INTERNAL));

        assertThat(primaryResponse.getItemId(), equalTo(0));
        assertThat(primaryResponse.getId(), equalTo("id"));
        assertThat(primaryResponse.getOpType(), equalTo(DocWriteRequest.OpType.DELETE));
        assertFalse(primaryResponse.isFailed());

        assertThat(response.getResult(), equalTo(DocWriteResponse.Result.NOT_FOUND));
        assertThat(response.getShardId(), equalTo(shard.shardId()));
        assertThat(response.getIndex(), equalTo("index"));
        assertThat(response.getType(), equalTo("_doc"));
        assertThat(response.getId(), equalTo("id"));
        assertThat(response.getVersion(), equalTo(1L));
        assertThat(response.getSeqNo(), equalTo(0L));
        assertThat(response.forcedRefresh(), equalTo(false));

        // Now do the same after indexing the document, it should now find and delete the document
        indexDoc(shard, "_doc", "id", "{}");

        writeRequest = new DeleteRequest("index", "_doc", "id");
        items[0] = new BulkItemRequest(0, writeRequest);
        bulkShardRequest = new BulkShardRequest(shardId, RefreshPolicy.NONE, items);

        location = context.getLocationToSync();

        context = new PrimaryExecutionContext(bulkShardRequest, shard);
        context.advance();
        TransportShardBulkAction.executeBulkItemRequest(context, updateHelper, threadPool::absoluteTimeInMillis,
            new NoopMappingUpdatePerformer());

        // Translog changes, because the document was deleted
        assertThat(context.getLocationToSync(), not(location));

        replicaRequest = bulkShardRequest.items()[0];
        replicaDeleteRequest = replicaRequest.request();
        primaryResponse = replicaRequest.getPrimaryResponse();
        response = primaryResponse.getResponse();

        // Any version can be matched on replica
        assertThat(replicaDeleteRequest.version(), equalTo(Versions.MATCH_ANY));
        assertThat(replicaDeleteRequest.versionType(), equalTo(VersionType.INTERNAL));

        assertThat(primaryResponse.getItemId(), equalTo(0));
        assertThat(primaryResponse.getId(), equalTo("id"));
        assertThat(primaryResponse.getOpType(), equalTo(DocWriteRequest.OpType.DELETE));
        assertFalse(primaryResponse.isFailed());

        assertThat(response.getResult(), equalTo(DocWriteResponse.Result.DELETED));
        assertThat(response.getShardId(), equalTo(shard.shardId()));
        assertThat(response.getIndex(), equalTo("index"));
        assertThat(response.getType(), equalTo("_doc"));
        assertThat(response.getId(), equalTo("id"));
        assertThat(response.getVersion(), equalTo(3L));
        assertThat(response.getSeqNo(), equalTo(2L));
        assertThat(response.forcedRefresh(), equalTo(false));

        assertDocCount(shard, 0);
        closeShards(shard);
    }

    public void testNoopUpdateRequest() throws Exception {
        DocWriteRequest<UpdateRequest> writeRequest = new UpdateRequest("index", "_doc", "id")
            .doc(Requests.INDEX_CONTENT_TYPE, "field", "value");
        BulkItemRequest primaryRequest = new BulkItemRequest(0, writeRequest);

        DocWriteResponse noopUpdateResponse = new UpdateResponse(shardId, "_doc", "id", 0,
            DocWriteResponse.Result.NOOP);

        IndexShard shard = mock(IndexShard.class);

        UpdateHelper updateHelper = mock(UpdateHelper.class);
        when(updateHelper.prepare(any(), eq(shard), any())).thenReturn(
            new UpdateHelper.Result(noopUpdateResponse, DocWriteResponse.Result.NOOP,
                Collections.singletonMap("field", "value"), Requests.INDEX_CONTENT_TYPE));

        BulkItemRequest[] items = new BulkItemRequest[]{primaryRequest};
        BulkShardRequest bulkShardRequest =
            new BulkShardRequest(shardId, RefreshPolicy.NONE, items);

        PrimaryExecutionContext context = new PrimaryExecutionContext(bulkShardRequest, shard);
        context.advance();
        TransportShardBulkAction.executeBulkItemRequest(context, updateHelper, threadPool::absoluteTimeInMillis,
            new NoopMappingUpdatePerformer());

        assertTrue(context.isFinalized());

        // Basically nothing changes in the request since it's a noop
        assertThat(context.getLocationToSync(), nullValue());
        BulkItemResponse primaryResponse = bulkShardRequest.items()[0].getPrimaryResponse();
        assertThat(primaryResponse.getItemId(), equalTo(0));
        assertThat(primaryResponse.getId(), equalTo("id"));
        assertThat(primaryResponse.getOpType(), equalTo(DocWriteRequest.OpType.UPDATE));
        assertThat(primaryResponse.getResponse(), equalTo(noopUpdateResponse));
        assertThat(primaryResponse.getResponse().getResult(),
            equalTo(DocWriteResponse.Result.NOOP));
        assertThat(primaryResponse.getResponse().getSeqNo(), equalTo(SequenceNumbers.UNASSIGNED_SEQ_NO));
    }

    public void testUpdateRequestWithFailure() throws Exception {
        IndexSettings indexSettings = new IndexSettings(indexMetaData(), Settings.EMPTY);
        DocWriteRequest<UpdateRequest> writeRequest = new UpdateRequest("index", "_doc", "id")
            .doc(Requests.INDEX_CONTENT_TYPE, "field", "value");
        BulkItemRequest primaryRequest = new BulkItemRequest(0, writeRequest);

        IndexRequest updateResponse = new IndexRequest("index", "_doc", "id").source(Requests.INDEX_CONTENT_TYPE, "field", "value");

        Exception err = new ElasticsearchException("I'm dead <(x.x)>");
        Engine.IndexResult indexResult = new Engine.IndexResult(err, 0, 0);
        IndexShard shard = mock(IndexShard.class);
        when(shard.applyIndexOperationOnPrimary(anyLong(), any(), any(), anyLong(), anyBoolean())).thenReturn(indexResult);
        when(shard.indexSettings()).thenReturn(indexSettings);

        UpdateHelper updateHelper = mock(UpdateHelper.class);
        when(updateHelper.prepare(any(), eq(shard), any())).thenReturn(
            new UpdateHelper.Result(updateResponse, randomBoolean() ? DocWriteResponse.Result.CREATED : DocWriteResponse.Result.UPDATED,
                Collections.singletonMap("field", "value"), Requests.INDEX_CONTENT_TYPE));

        BulkItemRequest[] items = new BulkItemRequest[]{primaryRequest};
        BulkShardRequest bulkShardRequest =
            new BulkShardRequest(shardId, RefreshPolicy.NONE, items);


        PrimaryExecutionContext context = new PrimaryExecutionContext(bulkShardRequest, shard);
        context.advance();
        TransportShardBulkAction.executeBulkItemRequest(context, updateHelper, threadPool::absoluteTimeInMillis,
            new NoopMappingUpdatePerformer());

        // Since this was not a conflict failure, the primary response
        // should be filled out with the failure information
        assertNull(context.getLocationToSync());
        BulkItemResponse primaryResponse = bulkShardRequest.items()[0].getPrimaryResponse();
        assertThat(primaryResponse.getItemId(), equalTo(0));
        assertThat(primaryResponse.getId(), equalTo("id"));
        assertThat(primaryResponse.getOpType(), equalTo(DocWriteRequest.OpType.INDEX));
        assertTrue(primaryResponse.isFailed());
        assertThat(primaryResponse.getFailureMessage(), containsString("I'm dead <(x.x)>"));
        BulkItemResponse.Failure failure = primaryResponse.getFailure();
        assertThat(failure.getIndex(), equalTo("index"));
        assertThat(failure.getType(), equalTo("_doc"));
        assertThat(failure.getId(), equalTo("id"));
        assertThat(failure.getCause(), equalTo(err));
        assertThat(failure.getStatus(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));
    }


    public void testUpdateRequestWithConflictFailure() throws Exception {
        IndexSettings indexSettings = new IndexSettings(indexMetaData(), Settings.EMPTY);
        DocWriteRequest<UpdateRequest> writeRequest = new UpdateRequest("index", "_doc", "id")
            .doc(Requests.INDEX_CONTENT_TYPE, "field", "value");
        BulkItemRequest primaryRequest = new BulkItemRequest(0, writeRequest);

        IndexRequest updateResponse = new IndexRequest("index", "_doc", "id").source(Requests.INDEX_CONTENT_TYPE, "field", "value");

        Exception err = new VersionConflictEngineException(shardId, "_doc", "id",
            "I'm conflicted <(;_;)>");
        Engine.IndexResult indexResult = new Engine.IndexResult(err, 0, 0);
        IndexShard shard = mock(IndexShard.class);
        when(shard.applyIndexOperationOnPrimary(anyLong(), any(), any(), anyLong(), anyBoolean())).thenReturn(indexResult);
        when(shard.indexSettings()).thenReturn(indexSettings);

        UpdateHelper updateHelper = mock(UpdateHelper.class);
        when(updateHelper.prepare(any(), eq(shard), any())).thenReturn(
            new UpdateHelper.Result(updateResponse, randomBoolean() ? DocWriteResponse.Result.CREATED : DocWriteResponse.Result.UPDATED,
                Collections.singletonMap("field", "value"), Requests.INDEX_CONTENT_TYPE));

        BulkItemRequest[] items = new BulkItemRequest[]{primaryRequest};
        BulkShardRequest bulkShardRequest =
            new BulkShardRequest(shardId, RefreshPolicy.NONE, items);


        PrimaryExecutionContext context = new PrimaryExecutionContext(bulkShardRequest, shard);
        context.advance();
        TransportShardBulkAction.executeBulkItemRequest(context, updateHelper, threadPool::absoluteTimeInMillis,
            new NoopMappingUpdatePerformer());

        assertNull(context.getLocationToSync());
        BulkItemResponse primaryResponse = bulkShardRequest.items()[0].getPrimaryResponse();
        assertThat(primaryResponse.getItemId(), equalTo(0));
        assertThat(primaryResponse.getId(), equalTo("id"));
        assertThat(primaryResponse.getOpType(), equalTo(DocWriteRequest.OpType.INDEX));
        assertTrue(primaryResponse.isFailed());
        assertThat(primaryResponse.getFailureMessage(), containsString("I'm conflicted <(;_;)>"));
        BulkItemResponse.Failure failure = primaryResponse.getFailure();
        assertThat(failure.getIndex(), equalTo("index"));
        assertThat(failure.getType(), equalTo("_doc"));
        assertThat(failure.getId(), equalTo("id"));
        assertThat(failure.getCause(), equalTo(err));
        assertThat(failure.getStatus(), equalTo(RestStatus.CONFLICT));
    }

    public void testUpdateRequestWithSuccess() throws Exception {
        IndexSettings indexSettings = new IndexSettings(indexMetaData(), Settings.EMPTY);
        DocWriteRequest<UpdateRequest> writeRequest = new UpdateRequest("index", "_doc", "id")
            .doc(Requests.INDEX_CONTENT_TYPE, "field", "value");
        BulkItemRequest primaryRequest = new BulkItemRequest(0, writeRequest);

        IndexRequest updateResponse = new IndexRequest("index", "_doc", "id").source(Requests.INDEX_CONTENT_TYPE, "field", "value");

        boolean created = randomBoolean();
        Translog.Location resultLocation = new Translog.Location(42, 42, 42);
        Engine.IndexResult indexResult = new FakeIndexResult(1, 13, created, resultLocation);
        IndexShard shard = mock(IndexShard.class);
        when(shard.applyIndexOperationOnPrimary(anyLong(), any(), any(), anyLong(), anyBoolean())).thenReturn(indexResult);
        when(shard.indexSettings()).thenReturn(indexSettings);

        UpdateHelper updateHelper = mock(UpdateHelper.class);
        when(updateHelper.prepare(any(), eq(shard), any())).thenReturn(
            new UpdateHelper.Result(updateResponse, created ? DocWriteResponse.Result.CREATED : DocWriteResponse.Result.UPDATED,
                Collections.singletonMap("field", "value"), Requests.INDEX_CONTENT_TYPE));

        BulkItemRequest[] items = new BulkItemRequest[]{primaryRequest};
        BulkShardRequest bulkShardRequest =
            new BulkShardRequest(shardId, RefreshPolicy.NONE, items);


        PrimaryExecutionContext context = new PrimaryExecutionContext(bulkShardRequest, shard);
        context.advance();
        TransportShardBulkAction.executeBulkItemRequest(context, updateHelper, threadPool::absoluteTimeInMillis,
            new NoopMappingUpdatePerformer());

        // Check that the translog is successfully advanced
        assertThat(context.getLocationToSync(), equalTo(resultLocation));
        assertThat(bulkShardRequest.items()[0].request(), equalTo(updateResponse));
        // Since this was not a conflict failure, the primary response
        // should be filled out with the failure information
        BulkItemResponse primaryResponse = bulkShardRequest.items()[0].getPrimaryResponse();
        assertThat(primaryResponse.getItemId(), equalTo(0));
        assertThat(primaryResponse.getId(), equalTo("id"));
        assertThat(primaryResponse.getOpType(), equalTo(DocWriteRequest.OpType.INDEX));
        DocWriteResponse response = primaryResponse.getResponse();
        assertThat(response.status(), equalTo(created ? RestStatus.CREATED : RestStatus.OK));
        assertThat(response.getSeqNo(), equalTo(13L));
    }

    public void testUpdateWithDelete() throws Exception {
        IndexSettings indexSettings = new IndexSettings(indexMetaData(), Settings.EMPTY);
        DocWriteRequest<UpdateRequest> writeRequest = new UpdateRequest("index", "_doc", "id")
            .doc(Requests.INDEX_CONTENT_TYPE, "field", "value");
        BulkItemRequest primaryRequest = new BulkItemRequest(0, writeRequest);

        DeleteRequest updateResponse = new DeleteRequest("index", "_doc", "id");

        boolean found = randomBoolean();
        Translog.Location resultLocation = new Translog.Location(42, 42, 42);
        final long resultSeqNo = 13;
        Engine.DeleteResult deleteResult = new FakeDeleteResult(1, resultSeqNo, found, resultLocation);
        IndexShard shard = mock(IndexShard.class);
        when(shard.applyDeleteOperationOnPrimary(anyLong(), any(), any(), any())).thenReturn(deleteResult);
        when(shard.indexSettings()).thenReturn(indexSettings);

        UpdateHelper updateHelper = mock(UpdateHelper.class);
        when(updateHelper.prepare(any(), eq(shard), any())).thenReturn(
            new UpdateHelper.Result(updateResponse, DocWriteResponse.Result.DELETED,
                Collections.singletonMap("field", "value"), Requests.INDEX_CONTENT_TYPE));

        BulkItemRequest[] items = new BulkItemRequest[]{primaryRequest};
        BulkShardRequest bulkShardRequest =
            new BulkShardRequest(shardId, RefreshPolicy.NONE, items);


        PrimaryExecutionContext context = new PrimaryExecutionContext(bulkShardRequest, shard);
        context.advance();
        TransportShardBulkAction.executeBulkItemRequest(context, updateHelper, threadPool::absoluteTimeInMillis,
            new NoopMappingUpdatePerformer());

        // Check that the translog is successfully advanced
        assertThat(context.getLocationToSync(), equalTo(resultLocation));
        assertThat(bulkShardRequest.items()[0].request(), equalTo(updateResponse));
        BulkItemResponse primaryResponse = bulkShardRequest.items()[0].getPrimaryResponse();
        assertThat(primaryResponse.getItemId(), equalTo(0));
        assertThat(primaryResponse.getId(), equalTo("id"));
        assertThat(primaryResponse.getOpType(), equalTo(DocWriteRequest.OpType.DELETE));
        DocWriteResponse response = primaryResponse.getResponse();
        assertThat(response.status(), equalTo(RestStatus.OK));
        assertThat(response.getSeqNo(), equalTo(resultSeqNo));
    }


    public void testFailureDuringUpdateProcessing() throws Exception {
        DocWriteRequest<UpdateRequest> writeRequest = new UpdateRequest("index", "_doc", "id")
            .doc(Requests.INDEX_CONTENT_TYPE, "field", "value");
        BulkItemRequest primaryRequest = new BulkItemRequest(0, writeRequest);

        IndexShard shard = mock(IndexShard.class);

        UpdateHelper updateHelper = mock(UpdateHelper.class);
        final ElasticsearchException err = new ElasticsearchException("oops");
        when(updateHelper.prepare(any(), eq(shard), any())).thenThrow(err);
        BulkItemRequest[] items = new BulkItemRequest[]{primaryRequest};
        BulkShardRequest bulkShardRequest =
            new BulkShardRequest(shardId, RefreshPolicy.NONE, items);


        PrimaryExecutionContext context = new PrimaryExecutionContext(bulkShardRequest, shard);
        context.advance();
        TransportShardBulkAction.executeBulkItemRequest(context, updateHelper, threadPool::absoluteTimeInMillis,
            new NoopMappingUpdatePerformer());

        assertNull(context.getLocationToSync());
        BulkItemResponse primaryResponse = bulkShardRequest.items()[0].getPrimaryResponse();
        assertThat(primaryResponse.getItemId(), equalTo(0));
        assertThat(primaryResponse.getId(), equalTo("id"));
        assertThat(primaryResponse.getOpType(), equalTo(DocWriteRequest.OpType.UPDATE));
        assertTrue(primaryResponse.isFailed());
        assertThat(primaryResponse.getFailureMessage(), containsString("oops"));
        BulkItemResponse.Failure failure = primaryResponse.getFailure();
        assertThat(failure.getIndex(), equalTo("index"));
        assertThat(failure.getType(), equalTo("_doc"));
        assertThat(failure.getId(), equalTo("id"));
        assertThat(failure.getCause(), equalTo(err));
        assertThat(failure.getStatus(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));
    }

    public void testTranslogPositionToSync() throws Exception {
        IndexShard shard = newStartedShard(true);

        BulkItemRequest[] items = new BulkItemRequest[randomIntBetween(2, 5)];
        for (int i = 0; i < items.length; i++) {
            DocWriteRequest<IndexRequest> writeRequest = new IndexRequest("index", "_doc", "id_" + i)
                .source(Requests.INDEX_CONTENT_TYPE)
                .opType(DocWriteRequest.OpType.INDEX);
            items[i] = new BulkItemRequest(i, writeRequest);
        }
        BulkShardRequest bulkShardRequest = new BulkShardRequest(shardId, RefreshPolicy.NONE, items);

        final long translogSizeInBytesBeforeIndexing = shard.translogStats().getTranslogSizeInBytes();
        UpdateHelper updateHelper = null;
        PlainActionFuture<WritePrimaryResult<BulkShardRequest, BulkShardResponse>> callback = new PlainActionFuture<>();
        WritePrimaryResult<BulkShardRequest, BulkShardResponse> result = TransportShardBulkAction.performOnPrimary(
            bulkShardRequest, shard, updateHelper, threadPool::absoluteTimeInMillis, new NoopMappingUpdatePerformer(),
            new ClusterStateObserver(clusterService, logger, threadPool.getThreadContext()), clusterService.localNode());

        final long translogSizeInBytesAfterIndexing = shard.translogStats().getTranslogSizeInBytes();
        assertThat(result.location.translogLocation,
            equalTo(translogSizeInBytesAfterIndexing - translogSizeInBytesBeforeIndexing - 1));

        // if we sync the location, nothing else is unsynced
        CountDownLatch latch = new CountDownLatch(1);
        shard.sync(result.location, e -> {
            if (e != null) {
                throw new AssertionError(e);
            }
            latch.countDown();
        });

        latch.await();
        assertFalse(shard.isSyncNeeded());

        closeShards(shard);
    }
    //    public void testCalculateTranslogLocation() throws Exception {
//        final Translog.Location original = new Translog.Location(0, 0, 0);
//
//        DocWriteRequest<IndexRequest> writeRequest = new IndexRequest("index", "_doc", "id")
//            .source(Requests.INDEX_CONTENT_TYPE);
//        BulkItemRequest replicaRequest = new BulkItemRequest(0, writeRequest);
//        BulkItemResultHolder results = new BulkItemResultHolder(null, null, replicaRequest);
//
//        assertThat(TransportShardBulkAction.calculateTranslogLocation(original, results),
//                equalTo(original));
//
//        boolean created = randomBoolean();
//        DocWriteResponse indexResponse = new IndexResponse(shardId, "_doc", "id", 1, 17, 1, created);
//        Translog.Location newLocation = new Translog.Location(1, 1, 1);
//        final long version = randomNonNegativeLong();
//        final long seqNo = randomNonNegativeLong();
//        Engine.IndexResult indexResult = new IndexResultWithLocation(version, seqNo, created, newLocation);
//        results = new BulkItemResultHolder(indexResponse, indexResult, replicaRequest);
//        assertThat(TransportShardBulkAction.calculateTranslogLocation(original, results),
//                equalTo(newLocation));
//
//    }
//
    public void testNoOpReplicationOnPrimaryDocumentFailure() throws Exception {
        final IndexShard shard = spy(newStartedShard(false));
        BulkItemRequest itemRequest = new BulkItemRequest(0, new IndexRequest("index", "_doc").source(Requests.INDEX_CONTENT_TYPE));
        final String failureMessage = "simulated primary failure";
        final IOException exception = new IOException(failureMessage);
        itemRequest.setPrimaryResponse(new BulkItemResponse(0,
            randomFrom(
                DocWriteRequest.OpType.CREATE,
                DocWriteRequest.OpType.DELETE,
                DocWriteRequest.OpType.INDEX
            ),
            new BulkItemResponse.Failure("index", "_doc", "1",
                exception, 1L)
        ));
        BulkItemRequest[] itemRequests = new BulkItemRequest[1];
        itemRequests[0] = itemRequest;
        BulkShardRequest bulkShardRequest = new BulkShardRequest(
            shard.shardId(), RefreshPolicy.NONE, itemRequests);
        TransportShardBulkAction.performOnReplica(bulkShardRequest, shard);
        verify(shard, times(1)).markSeqNoAsNoop(1, exception.toString());
        closeShards(shard);
    }

    public void testRetries() throws Exception {
        IndexSettings indexSettings = new IndexSettings(indexMetaData(), Settings.EMPTY);
        UpdateRequest writeRequest = new UpdateRequest("index", "_doc", "id")
            .doc(Requests.INDEX_CONTENT_TYPE, "field", "value");
        // the beating will continue until success has come.
        writeRequest.retryOnConflict(Integer.MAX_VALUE);
        BulkItemRequest primaryRequest = new BulkItemRequest(0, writeRequest);

        IndexRequest updateResponse = new IndexRequest("index", "_doc", "id").source(Requests.INDEX_CONTENT_TYPE, "field", "value");

        Exception err = new VersionConflictEngineException(shardId, "_doc", "id",
            "I'm conflicted <(;_;)>");
        Engine.IndexResult conflictedResult = new Engine.IndexResult(err, 0, 0);
        Engine.IndexResult mappingUpdate =
            new Engine.IndexResult(new Mapping(null, null, new MetadataFieldMapper[0], Collections.emptyMap()));
        Translog.Location resultLocation = new Translog.Location(42, 42, 42);
        Engine.IndexResult success = new FakeIndexResult(1, 13, true, resultLocation);

        IndexShard shard = mock(IndexShard.class);
        when(shard.applyIndexOperationOnPrimary(anyLong(), any(), any(), anyLong(), anyBoolean())).thenAnswer(ir -> {
            if (randomBoolean()) {
                return conflictedResult;
            }
            if (randomBoolean()) {
                return mappingUpdate;
            } else {
                return success;
            }
        });
        when(shard.indexSettings()).thenReturn(indexSettings);

        UpdateHelper updateHelper = mock(UpdateHelper.class);
        when(updateHelper.prepare(any(), eq(shard), any())).thenReturn(
            new UpdateHelper.Result(updateResponse, randomBoolean() ? DocWriteResponse.Result.CREATED : DocWriteResponse.Result.UPDATED,
                Collections.singletonMap("field", "value"), Requests.INDEX_CONTENT_TYPE));

        BulkItemRequest[] items = new BulkItemRequest[]{primaryRequest};
        BulkShardRequest bulkShardRequest =
            new BulkShardRequest(shardId, RefreshPolicy.NONE, items);


        final ClusterStateObserver observer = mock(ClusterStateObserver.class);
        doAnswer(invocationOnMock -> {
            ((ClusterStateObserver.Listener) invocationOnMock.getArguments()[0]).onNewClusterState(null);
            return null;
        })
            .when(observer).waitForNextChange(any());
        WritePrimaryResult<BulkShardRequest, BulkShardResponse> result = TransportShardBulkAction.performOnPrimary(
            bulkShardRequest, shard, updateHelper, threadPool::absoluteTimeInMillis, new NoopMappingUpdatePerformer(),
            observer, clusterService.localNode());

        assertThat(result.location, equalTo(resultLocation));
        BulkItemResponse primaryResponse = result.replicaRequest().items()[0].getPrimaryResponse();
        assertThat(primaryResponse.getItemId(), equalTo(0));
        assertThat(primaryResponse.getId(), equalTo("id"));
        assertThat(primaryResponse.getOpType(), equalTo(DocWriteRequest.OpType.INDEX));
        DocWriteResponse response = primaryResponse.getResponse();
        assertThat(response.status(), equalTo(RestStatus.CREATED));
        assertThat(response.getSeqNo(), equalTo(13L));
    }

    /**
     * Fake IndexResult that has a settable translog location
     */
    private static class FakeIndexResult extends Engine.IndexResult {

        private final Translog.Location location;

        protected FakeIndexResult(long version, long seqNo, boolean created, Translog.Location location) {
            super(version, seqNo, created);
            this.location = location;
        }

        @Override
        public Translog.Location getTranslogLocation() {
            return this.location;
        }
    }

    /**
     * Fake DeleteResult that has a settable translog location
     */
    private static class FakeDeleteResult extends Engine.DeleteResult {

        private final Translog.Location location;

        protected FakeDeleteResult(long version, long seqNo, boolean found, Translog.Location location) {
            super(version, seqNo, found);
            this.location = location;
        }

        @Override
        public Translog.Location getTranslogLocation() {
            return this.location;
        }
    }

    /** Doesn't perform any mapping updates */
    public static class NoopMappingUpdatePerformer implements MappingUpdatePerformer {
        @Override
        public void updateMappings(Mapping update, ShardId shardId, String type) {
        }
    }

    /** Always throw the given exception */
    private class ThrowingMappingUpdatePerformer implements MappingUpdatePerformer {
        private final RuntimeException e;

        ThrowingMappingUpdatePerformer(RuntimeException e) {
            this.e = e;
        }

        @Override
        public void updateMappings(Mapping update, ShardId shardId, String type) {
            throw e;
        }
    }
}
