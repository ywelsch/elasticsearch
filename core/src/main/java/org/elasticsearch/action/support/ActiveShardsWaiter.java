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

package org.elasticsearch.action.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * This class provides primitives for waiting for a configured number of shards
 * to become active before sending a response on an {@link ActionListener}.
 */
public class ActiveShardsWaiter extends AbstractComponent {

    private final ClusterService clusterService;
    private final ThreadPool threadPool;

    public ActiveShardsWaiter(final Settings settings, final ClusterService clusterService, final ThreadPool threadPool) {
        super(settings);
        this.clusterService = clusterService;
        this.threadPool = threadPool;
    }

    /**
     * Creates an {@link ActionListener} that responds to a cluster state update upon index creation and
     * waits on the specified number of active shards to be started before sending the action's response
     * over its listener.
     *
     * @param request the index creation request
     * @param actionListener the main listener that is listening for responses from the index creation event
     * @param onResult blablalblaal
     * @return ActionListener for responding to index creation cluster state events and sending the action response
     *         over the main listener, after waiting for the requested number of shards to be active
     */
    public <T> ActionListener<ClusterStateUpdateResponse> wrapUpdateListenerWithWaiting(final CreateIndexClusterStateUpdateRequest request,
                                                                                    final ActionListener<T> actionListener,
                                                                                    final BiFunction<Boolean, Boolean, T> onResult) {
        return new ActionListener<ClusterStateUpdateResponse>() {
            @Override
            public void onResponse(ClusterStateUpdateResponse response) {
                if (response.isAcknowledged()) {
                    final String indexName = request.index();
                    final ActiveShardCount waitForActiveShards = request.waitForActiveShards();
                    final TimeValue timeout = request.masterNodeTimeout();
                    try {
                        // the cluster state update with the created index has been acknowledged, now wait for the
                        // configured number of active shards to be allocated before returning, as that is when indexing
                        // operations can take place on the newly created index
                        if (waitForActiveShards == ActiveShardCount.NONE) {
                            // not waiting, so just run whatever we were to run when the waiting is
                            actionListener.onResponse(onResult.apply(true, false));
                            return;
                        }

                        // wait for the configured number of active shards to be allocated before returning
                        final ClusterStateObserver observer =
                            new ClusterStateObserver(clusterService, logger, threadPool.getThreadContext());
                        final ClusterStateObserver.ChangePredicate shardsAllocatedPredicate = new ClusterStateObserver.ChangePredicate() {
                            @Override
                            public boolean apply(ClusterState previousState, ClusterState.ClusterStateStatus previousStatus,
                                                 ClusterState newState, ClusterState.ClusterStateStatus newStatus) {
                                return waitForActiveShards.enoughShardsActive(newState, indexName, settings);
                            }

                            @Override
                            public boolean apply(ClusterChangedEvent changedEvent) {
                                return waitForActiveShards.enoughShardsActive(changedEvent.state(), indexName, settings);
                            }
                        };

                        final ClusterStateObserver.Listener observerListener = new ClusterStateObserver.Listener() {
                            @Override
                            public void onNewClusterState(ClusterState state) {
                                actionListener.onResponse(onResult.apply(true, false));
                            }

                            @Override
                            public void onClusterServiceClose() {
                                logger.debug("[{}] cluster service closed while waiting for enough shards to be started.", indexName);
                                actionListener.onFailure(new NodeClosedException(clusterService.localNode()));
                            }

                            @Override
                            public void onTimeout(TimeValue timeout) {
                                actionListener.onResponse(onResult.apply(true, true));
                            }
                        };

                        observer.waitForNextChange(observerListener, shardsAllocatedPredicate, timeout);

                    } catch (Exception ex) {
                        logger.debug("[{}] index creation failed on waiting for shards", request.index());
                        actionListener.onFailure(ex);
                    }
                } else {
                    actionListener.onResponse(onResult.apply(false, false));
                }
            }

            @Override
            public void onFailure(Throwable t) {
                if (t instanceof IndexAlreadyExistsException) {
                    logger.trace("[{}] failed to create", t, request.index());
                } else {
                    logger.debug("[{}] failed to create", t, request.index());
                }
                actionListener.onFailure(t);
            }
        };
    }

}
