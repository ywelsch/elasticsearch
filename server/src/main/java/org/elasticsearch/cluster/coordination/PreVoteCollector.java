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

package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationState.VoteCollection;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;

import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentSet;

public class PreVoteCollector extends AbstractComponent {

    public static final String REQUEST_PRE_VOTE_ACTION_NAME = "internal:cluster/request_pre_vote";

    private final long electionId;

    private final Set<DiscoveryNode> preVotesReceived = newConcurrentSet();
    private final AtomicBoolean electionStarted = new AtomicBoolean();
    private final AtomicBoolean stopped = new AtomicBoolean();
    private final AtomicLong maxTermSeen;
    private final PreVoteResponse localPreVoteResponse;
    private final PreVoteRequest preVoteRequest;
    private final ClusterState clusterState;
    private final TransportService transportService;
    private final LongConsumer startElection;

    public PreVoteCollector(Settings settings, long electionId, PreVoteResponse localPreVoteResponse, TransportService transportService,
                     ClusterState clusterState, LongConsumer startElection) {
        super(settings);
        this.electionId = electionId;
        this.localPreVoteResponse = localPreVoteResponse;
        this.transportService = transportService;
        this.clusterState = clusterState;
        this.startElection = startElection;
        final long currentTerm = localPreVoteResponse.getCurrentTerm();
        preVoteRequest = new PreVoteRequest(transportService.getLocalNode(), currentTerm);
        maxTermSeen = new AtomicLong(currentTerm);
    }

    public void stop() {
        stopped.set(true);
    }

    public void start(Iterable<DiscoveryNode> broadCastNodes) {
        logger.debug("{} starting", this);

        broadCastNodes.forEach(n -> transportService.sendRequest(n, REQUEST_PRE_VOTE_ACTION_NAME, preVoteRequest,
            new TransportResponseHandler<PreVoteResponse>() {
                @Override
                public void handleResponse(PreVoteResponse response) {
                    handlePreVoteResponse(response, n);
                }

                @Override
                public void handleException(TransportException exp) {
                    if (exp.getRootCause() instanceof CoordinationStateRejectedException) {
                        logger.debug("{} failed: {}", this, exp.getRootCause().getMessage());
                    } else {
                        logger.debug(new ParameterizedMessage("{} failed", this), exp);
                    }
                }

                @Override
                public String executor() {
                    return Names.GENERIC;
                }

                @Override
                public String toString() {
                    return "TransportResponseHandler{" + PreVoteCollector.this + ", node=" + n + '}';
                }
            }));
    }

    private void handlePreVoteResponse(PreVoteResponse response, DiscoveryNode sender) {
        if (stopped.get()) {
            logger.debug("{} ignoring {} from {} as current prevote collector is stopped", this, response, sender);
            return;
        }

        final long currentMaxTermSeen = maxTermSeen.accumulateAndGet(response.getCurrentTerm(), Math::max);

        if (response.getLastAcceptedTerm() > localPreVoteResponse.getLastAcceptedTerm()
            || (response.getLastAcceptedTerm() == localPreVoteResponse.getLastAcceptedTerm()
            && response.getLastAcceptedVersion() > localPreVoteResponse.getLastAcceptedVersion())) {
            logger.debug("{} ignoring {} from {} as it is fresher", this, response, sender);
            return;
        }

        preVotesReceived.add(sender);
        final VoteCollection voteCollection = new VoteCollection();
        preVotesReceived.forEach(voteCollection::addVote);

        if (CoordinationState.isElectionQuorum(voteCollection, clusterState) == false) {
            logger.debug("{} added {} from {}, no quorum yet", this, response, sender);
            return;
        }

        if (electionStarted.compareAndSet(false, true) == false) {
            logger.debug("{} added {} from {} but election has already started", this, response, sender);
            return;
        }

        logger.debug("{} added {} from {}, starting election in term > {}", this, response, sender, currentMaxTermSeen);
        startElection.accept(currentMaxTermSeen);
    }

    @Override
    public String toString() {
        return "PreVoteCollector{" +
            "electionId=" + electionId +
            ", localPreVoteResponse=" + localPreVoteResponse +
            '}';
    }
}
