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

package org.elasticsearch.discovery.zen;

import org.elasticsearch.cluster.ClusterState;

public class ZenState {
    public static final long UNKNOWN_TERM = -1L;

    private final DiscoPhase discoPhase;
    private final long nodeTerm;
    private final long clusterStateTerm;
    private final ClusterState clusterState;


    public ZenState(DiscoPhase discoPhase, long nodeTerm, long clusterStateTerm, ClusterState clusterState) {
        this.discoPhase = discoPhase;
        this.nodeTerm = nodeTerm;
        this.clusterStateTerm = clusterStateTerm;
        this.clusterState = clusterState;
    }

    public long getNodeTerm() {
        return nodeTerm;
    }

    public long getClusterStateTerm() {
        return clusterStateTerm;
    }

    public ClusterState getClusterState() {
        return clusterState;
    }

    public DiscoPhase getDiscoPhase() {
        return discoPhase;
    }

    public ZenState withPhaseAndNodeTerm(DiscoPhase phase, long nodeTerm) {
        return new ZenState(phase, nodeTerm, clusterStateTerm, clusterState);
    }

    public ZenState withPhaseAndState(DiscoPhase phase, long clusterStateTerm, ClusterState clusterState) {
        return new ZenState(phase, nodeTerm, clusterStateTerm, clusterState);
    }
}
