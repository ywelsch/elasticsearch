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

package org.elasticsearch.discovery.zen.ping.local;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.discovery.zen.ping.PingContextProvider;
import org.elasticsearch.discovery.zen.ping.ZenPing;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentMap;

public class LocalPing extends AbstractLifecycleComponent<ZenPing> implements ZenPing {
    private final ClusterName clusterName;

    private volatile PingContextProvider contextProvider;

    private static final ConcurrentMap<ClusterName, ClusterGroup> clusterGroups = ConcurrentCollections.newConcurrentMap();

    @Inject
    public LocalPing(Settings settings, ClusterName clusterName) {
        super(settings);
        this.clusterName = clusterName;
    }

    @Override
    protected void doStart() {
        synchronized (clusterGroups) {
            ClusterGroup clusterGroup = clusterGroups.get(clusterName);
            if (clusterGroup == null) {
                clusterGroup = new ClusterGroup();
                clusterGroups.put(clusterName, clusterGroup);
            }
            logger.debug("Connected to cluster [{}]", clusterName);

            clusterGroup.members().add(this);
        }
    }

    @Override
    protected void doStop() {
        synchronized (clusterGroups) {
            ClusterGroup clusterGroup = clusterGroups.get(clusterName);
            if (clusterGroup == null) {
                logger.warn("Illegal state, should not have an empty cluster group when stopping, I should be there at the very least...");
                return;
            }
            clusterGroup.members().remove(this);
            if (clusterGroup.members().isEmpty()) {
                // no more members, remove and return
                clusterGroups.remove(clusterName);
                return;
            }
        }
    }

    @Override
    protected void doClose() {

    }

    @Override
    public void setPingContextProvider(PingContextProvider contextProvider) {
        this.contextProvider = contextProvider;
    }

    @Override
    public void ping(PingListener listener, TimeValue timeout) {
        ClusterGroup clusterGroup = clusterGroups.get(clusterName);
        if (clusterGroup == null) {
            listener.onPing(null);
            return;
        }
        // ignore timeout, we directly respond
        List<PingResponse> responses = new ArrayList<>();
        for (LocalPing localPing : clusterGroup.members()) {
            DiscoveryNodes discoNodes = localPing.contextProvider.nodes();
            responses.add(new PingResponse(
                    discoNodes.localNode(), discoNodes.masterNode(), localPing.clusterName, localPing.contextProvider.nodeHasJoinedClusterOnce()));
        }
        listener.onPing(responses.toArray(new PingResponse[responses.size()]));
    }

    private class ClusterGroup {

        private Queue<LocalPing> members = ConcurrentCollections.newQueue();

        Queue<LocalPing> members() {
            return members;
        }
    }
}
