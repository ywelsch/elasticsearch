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

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState.VotingConfiguration;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.elasticsearch.cluster.coordination.PreVoteCollector.REQUEST_PRE_VOTE_ACTION_NAME;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class PreVoteCollectorTests extends ESTestCase {

    private DeterministicTaskQueue deterministicTaskQueue;
    private boolean electionOccurred = false;
    private DiscoveryNode localNode;
    private Map<DiscoveryNode, PreVoteResponse> responsesByNode = new HashMap<>();
    private long lastElectionMaxTermSeen;
    private TransportService transportService;
    final Settings settings = Settings.builder().put(NODE_NAME_SETTING.getKey(), "node").build();

    @Before
    public void createObjects() {
        deterministicTaskQueue = new DeterministicTaskQueue(settings);
        final Transport capturingTransport = new CapturingTransport() {
            @Override
            protected void onSendRequest(final long requestId, final String action, final TransportRequest request,
                                         final DiscoveryNode node) {
                super.onSendRequest(requestId, action, request, node);
                assertThat(action, is(REQUEST_PRE_VOTE_ACTION_NAME));
                assertThat(request, instanceOf(PreVoteRequest.class));
                assertThat(node, not(equalTo(localNode)));
                PreVoteRequest preVoteRequest = (PreVoteRequest) request;
                assertThat(preVoteRequest.getSourceNode(), equalTo(localNode));
                deterministicTaskQueue.scheduleNow(new Runnable() {
                    @Override
                    public void run() {
                        final PreVoteResponse response = responsesByNode.get(node);
                        if (response == null) {
                            handleRemoteError(requestId, new ConnectTransportException(node, "no response"));
                        } else {
                            handleResponse(requestId, response);
                        }
                    }

                    @Override
                    public String toString() {
                        return "response to " + request + " from " + node;
                    }
                });
            }
        };
        localNode = new DiscoveryNode("local-node", buildNewFakeTransportAddress(), Version.CURRENT);
        responsesByNode.put(localNode, new PreVoteResponse(3, 2, 1));
        transportService = new TransportService(settings, capturingTransport,
            deterministicTaskQueue.getThreadPool(), TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundTransportAddress -> localNode, null, emptySet());
        transportService.start();
        transportService.acceptIncomingRequests();

        transportService.registerRequestHandler(REQUEST_PRE_VOTE_ACTION_NAME, Names.GENERIC, PreVoteRequest::new,
            (request, channel, task) -> {
                assertThat(request.getSourceNode(), equalTo(localNode));
                // this is just used for local messages
                channel.sendResponse(getLocalPreVoteResponse());
            });
    }

    private PreVoteCollector createPreVoteCollector(VotingConfiguration votingConfiguration) {
        return new PreVoteCollector(settings, randomNonNegativeLong(), getLocalPreVoteResponse(), transportService,
            CoordinationStateTests.clusterState(0, 0, localNode, votingConfiguration, votingConfiguration, 0),
            maxTermSeen -> {
                assert electionOccurred == false;
                electionOccurred = true;
                lastElectionMaxTermSeen = maxTermSeen;
            });
    }

    private PreVoteResponse getLocalPreVoteResponse() {
        return Objects.requireNonNull(responsesByNode.get(localNode));
    }

    private void runCollector() {
        deterministicTaskQueue.runAllRunnableTasks(random());
        assertFalse(deterministicTaskQueue.hasDeferredTasks());
        assertFalse(deterministicTaskQueue.hasRunnableTasks());
    }

    public void testStartsElectionIfLocalNodeIsOnlyNode() {
        createPreVoteCollector(new VotingConfiguration(singleton(localNode.getId()))).start(responsesByNode.keySet());
        runCollector();
        assertTrue(electionOccurred);
    }

    public void testStartsElectionIfLocalNodeIsQuorum() {
        final DiscoveryNode otherNode = new DiscoveryNode("other-node", buildNewFakeTransportAddress(), Version.CURRENT);
        responsesByNode.put(otherNode, new PreVoteResponse(3, 2, 1));
        createPreVoteCollector(new VotingConfiguration(singleton(localNode.getId()))).start(responsesByNode.keySet());
        runCollector();
        assertTrue(electionOccurred);
    }

    public void testStartsElectionIfOtherNodeIsQuorum() {
        final DiscoveryNode otherNode = new DiscoveryNode("other-node", buildNewFakeTransportAddress(), Version.CURRENT);
        responsesByNode.put(otherNode, new PreVoteResponse(3, 2, 1));
        createPreVoteCollector(new VotingConfiguration(singleton(otherNode.getId()))).start(responsesByNode.keySet());
        runCollector();
        assertTrue(electionOccurred);
    }

    public void testDoesNotStartsElectionIfOtherNodeIsQuorumAndDoesNotRespond() {
        final DiscoveryNode otherNode = new DiscoveryNode("other-node", buildNewFakeTransportAddress(), Version.CURRENT);
        responsesByNode.put(otherNode, null);
        createPreVoteCollector(new VotingConfiguration(singleton(otherNode.getId()))).start(responsesByNode.keySet());
        runCollector();
        assertFalse(electionOccurred);
    }

    public void testDoesNotStartElectionIfStopped() {
        final DiscoveryNode otherNode = new DiscoveryNode("other-node", buildNewFakeTransportAddress(), Version.CURRENT);
        responsesByNode.put(otherNode, new PreVoteResponse(3, 2, 1));
        PreVoteCollector preVoteCollector = createPreVoteCollector(new VotingConfiguration(singleton(otherNode.getId())));
        preVoteCollector.start(responsesByNode.keySet());
        preVoteCollector.stop();
        runCollector();
        assertFalse(electionOccurred);
    }

    public void testIgnoresPreVotesFromLaterTerms() {
        final DiscoveryNode otherNode = new DiscoveryNode("other-node", buildNewFakeTransportAddress(), Version.CURRENT);
        responsesByNode.put(otherNode, new PreVoteResponse(3, 3, 1));
        createPreVoteCollector(new VotingConfiguration(singleton(otherNode.getId()))).start(responsesByNode.keySet());
        runCollector();
        assertFalse(electionOccurred);
    }

    public void testIgnoresPreVotesFromLaterVersionInSameTerm() {
        final DiscoveryNode otherNode = new DiscoveryNode("other-node", buildNewFakeTransportAddress(), Version.CURRENT);
        responsesByNode.put(otherNode, new PreVoteResponse(3, 2, 2));
        createPreVoteCollector(new VotingConfiguration(singleton(otherNode.getId()))).start(responsesByNode.keySet());
        runCollector();
        assertFalse(electionOccurred);
    }

    public void testAcceptsPreVotesFromLaterVersionInEarlierTerms() {
        final DiscoveryNode otherNode = new DiscoveryNode("other-node", buildNewFakeTransportAddress(), Version.CURRENT);
        responsesByNode.put(otherNode, new PreVoteResponse(3, 1, 2));
        createPreVoteCollector(new VotingConfiguration(singleton(otherNode.getId()))).start(responsesByNode.keySet());
        runCollector();
        assertTrue(electionOccurred);
        assertThat(lastElectionMaxTermSeen, is(3L));
    }

    public void testReturnsMaximumSeenTerm() {
        final DiscoveryNode otherNode1 = new DiscoveryNode("other-node-1", buildNewFakeTransportAddress(), Version.CURRENT);
        final DiscoveryNode otherNode2 = new DiscoveryNode("other-node-2", buildNewFakeTransportAddress(), Version.CURRENT);
        responsesByNode.put(otherNode1, new PreVoteResponse(4, 2, 1));
        responsesByNode.put(otherNode2, new PreVoteResponse(5, 2, 1));
        createPreVoteCollector(new VotingConfiguration(new HashSet<>(Arrays.asList(otherNode1.getId(), otherNode2.getId()))))
            .start(responsesByNode.keySet());
        runCollector();
        assertTrue(electionOccurred);
        assertThat(lastElectionMaxTermSeen, is(5L));
    }
}
