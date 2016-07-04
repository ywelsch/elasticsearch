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
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.function.Consumer;

/**
 * This class enables waiting for a configured number of shards
 * to become active before proceeding with an action.
 */
public class WaitForActiveShardsMonitor extends AbstractComponent {

    private final ClusterService clusterService;
    private final ThreadPool threadPool;

    public WaitForActiveShardsMonitor(final Settings settings,
                                      final ClusterService clusterService,
                                      final ThreadPool threadPool) {
        super(settings);
        this.clusterService = clusterService;
        this.threadPool = threadPool;
    }

    /**
     * Waits for the specified number of shards to become active in the given index,
     * or times out with the specified timeout value.
     *
     * @param indexName the index to wait on active shards for
     * @param waitForActiveShards the number of active shards to wait to be started on the index
     * @param timeout the timeout value
     * @param listener the response listener
     * @param onPredicateAppliedOrTimeout the consumer function to execute upon a matching cluster state predicate, or on timeout;
     *                                    the boolean input value represents if a timeout occurred (if false, then it was a successful
     *                                    matching predicate)
     */
    public void waitOnShards(final String indexName,
                             final ActiveShardCount waitForActiveShards,
                             final TimeValue timeout,
                             final ActionListener<?> listener,
                             final Consumer<Boolean> onPredicateAppliedOrTimeout) {

        if (waitForActiveShards == ActiveShardCount.NONE) {
            // not waiting, so just run whatever we were to run when the waiting is
            onPredicateAppliedOrTimeout.accept(false);
        }

        // wait for the configured number of active shards to be allocated before returning
        final ClusterStateObserver observer = new ClusterStateObserver(clusterService, logger, threadPool.getThreadContext());
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
                onPredicateAppliedOrTimeout.accept(false);
            }

            @Override
            public void onClusterServiceClose() {
                logger.debug("[{}] cluster service closed while waiting for enough shards to be started.", indexName);
                listener.onFailure(new NodeClosedException(clusterService.localNode()));
            }

            @Override
            public void onTimeout(TimeValue timeout) {
                onPredicateAppliedOrTimeout.accept(true);
            }
        };

        observer.waitForNextChange(observerListener, shardsAllocatedPredicate, timeout);
    }

}
