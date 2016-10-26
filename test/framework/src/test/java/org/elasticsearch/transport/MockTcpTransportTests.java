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
package org.elasticsearch.transport;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class MockTcpTransportTests extends AbstractSimpleTransportTestCase {

    public static final String NAME = "indices:admin/settings/update";

    @Override
    protected MockTransportService build(Settings settings, Version version, ClusterSettings clusterSettings) {
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(Collections.emptyList());
        Transport transport = new MockTcpTransport(settings, threadPool, BigArrays.NON_RECYCLING_INSTANCE,
            new NoneCircuitBreakerService(), namedWriteableRegistry, new NetworkService(settings, Collections.emptyList()), version);
        MockTransportService mockTransportService = new MockTransportService(Settings.EMPTY, transport, threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR, clusterSettings);
        mockTransportService.start();
        return mockTransportService;
    }

    public void testBla() throws InterruptedException {
        serviceA.registerRequestHandler(NAME, TransportRequest.Empty::new, ThreadPool.Names.GENERIC,
            (request, channel) -> {
                try {
                    TransportResponseOptions responseOptions = TransportResponseOptions.builder().withCompress(true).build();
                    channel.sendResponse(TransportResponse.Empty.INSTANCE, responseOptions);
                } catch (IOException e) {
                    logger.error("Unexpected failure", e);
                    fail(e.getMessage());
                }
            });

        Thread[] threads = new Thread[50];
        for (int j = 0; j < threads.length; j++) {
            Thread thread = new Thread(() -> {
                for (int i = 0; i < 10000; i++) {
                    serviceB.shouldTraceAction(NAME);
//                    TransportFuture<TransportResponse.Empty> res = serviceB.submitRequest(nodeA, NAME,
//                        TransportRequest.Empty.INSTANCE, TransportRequestOptions.builder().withCompress(true).build(),
//                        new TransportResponseHandler<TransportResponse.Empty>() {
//                            @Override
//                            public TransportResponse.Empty newInstance() {
//                                return TransportResponse.Empty.INSTANCE;
//                            }
//
//                            @Override
//                            public String executor() {
//                                return ThreadPool.Names.GENERIC;
//                            }
//
//                            @Override
//                            public void handleResponse(TransportResponse.Empty response) {
//                            }
//
//                            @Override
//                            public void handleException(TransportException exp) {
//                                logger.error("Unexpected failure", exp);
//                                fail("got exception instead of a response: " + exp.getMessage());
//                            }
//                        });
//
//                    try {
//                        TransportResponse.Empty message = res.get();
//                        assertThat(message, notNullValue());
//                    } catch (Exception e) {
//                        assertThat(e.getMessage(), false, equalTo(true));
//                    }
                }
            });

            threads[j] = thread;
        }

        for (Thread thread : threads) {
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }


    }
}
