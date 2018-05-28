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

package org.elasticsearch.discovery.zen2;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.ack.AckedRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterApplier;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.zen.MembershipAction;
import org.elasticsearch.discovery.zen2.Messages.AbdicationRequest;
import org.elasticsearch.discovery.zen2.Messages.ApplyCommit;
import org.elasticsearch.discovery.zen2.Messages.HeartbeatRequest;
import org.elasticsearch.discovery.zen2.Messages.HeartbeatResponse;
import org.elasticsearch.discovery.zen2.Messages.Join;
import org.elasticsearch.discovery.zen2.Messages.LeaderCheckRequest;
import org.elasticsearch.discovery.zen2.Messages.LegislatorPublishResponse;
import org.elasticsearch.discovery.zen2.Messages.OfferJoin;
import org.elasticsearch.discovery.zen2.Messages.PrejoinHandoverRequest;
import org.elasticsearch.discovery.zen2.Messages.PublishRequest;
import org.elasticsearch.discovery.zen2.Messages.SeekJoins;
import org.elasticsearch.discovery.zen2.Messages.StartJoinRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.EmptyTransportResponseHandler;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.discovery.zen2.ConsensusStateTests.clusterState;
import static org.elasticsearch.discovery.zen2.ConsensusStateTests.setValue;
import static org.elasticsearch.discovery.zen2.ConsensusStateTests.value;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class ConcurrentLegislatorTests extends ESTestCase {

    //@TestLogging("org.elasticsearch.discovery.zen2:TRACE")
    public void testSimpleClusterFormation() throws Exception {
        final int numClusterNodes = randomIntBetween(1, 10);
        final List<ClusterNode> clusterNodes = new ArrayList<>(numClusterNodes);
        for (int i = 0; i < numClusterNodes; i++) {
            clusterNodes.add(new ClusterNode("node" + i));
        }

        try {
            for (ClusterNode node1 : clusterNodes) {
                for (ClusterNode node2 : clusterNodes) {
                    if (node1 != node2) {
                        node1.mockTransportService.connectToNode(node2.localNode);
                    }
                }
            }

            List<DiscoveryNode> allNodes = clusterNodes.stream().map(node -> node.localNode).collect(Collectors.toList());

            Set<String> votingNodes = randomSubsetOf(allNodes).stream().map(DiscoveryNode::getId).collect(Collectors.toSet());
            if (votingNodes.isEmpty()) {
                votingNodes = Collections.singleton(allNodes.get(0).getId());
            }
            ClusterState.VotingConfiguration initialConfig = new ClusterState.VotingConfiguration(votingNodes);

            clusterNodes.forEach(node -> node.initialise(initialConfig, () -> allNodes));

            assertBusy(() -> clusterNodes.forEach(node -> {
                assertTrue(node.getClusterState().isPresent());
                assertThat(node.getClusterState().get().nodes().getSize(), equalTo(numClusterNodes));
            }));

            DiscoveryNode masterNode = clusterNodes.get(0).getClusterState().get().nodes().getMasterNode();
            ClusterNode master = clusterNodes.stream().filter(n -> n.localNode.equals(masterNode)).findAny().get();

            PlainActionFuture<Void> fut = new PlainActionFuture<>();
            master.masterService.submitStateUpdateTask("change value to 13", new AckedClusterStateUpdateTask<Void>(
                new AckedRequest() {
                    @Override
                    public TimeValue ackTimeout() {
                        return TimeValue.timeValueMinutes(1);
                    }

                    @Override
                    public TimeValue masterNodeTimeout() {
                        return TimeValue.timeValueMinutes(1);
                    }
                }, fut) {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        return setValue(currentState, 13);
                    }

                    @Override
                    protected Void newResponse(boolean acknowledged) {
                        return null;
                    }
                });

            fut.get();

            clusterNodes.forEach(node -> assertThat(value(node.getClusterState().get()), equalTo(13L)));
        } finally {
            IOUtils.close(clusterNodes);
        }
    }

    class ClusterNode implements Closeable {
        private final Settings settings;
        private final DiscoveryNode localNode;
        private final ThreadPool threadPool;
        private final Object mutex = new Object();
        private final MasterService masterService;
        private final MockTransportService mockTransportService;
        private Legislator legislator;
        private final MockClusterApplier clusterApplier;

        ClusterNode(String name) {
            settings = Settings.builder()
                .put("node.name", name)
                .build();

            threadPool = new ThreadPool(settings);

            mockTransportService = MockTransportService.createNewService(settings, Version.CURRENT,
                threadPool, null);
            mockTransportService.start();
            mockTransportService.acceptIncomingRequests();

            localNode = mockTransportService.getLocalNode();

            masterService = new MasterService(settings, threadPool);

            clusterApplier = new MockClusterApplier(settings);
        }

        Optional<ClusterState> getClusterState() {
            synchronized (mutex) {
                return legislator.getLastCommittedState();
            }
        }

        void initialise(ClusterState.VotingConfiguration initialConfiguration, Supplier<List<DiscoveryNode>> nodeSupplier) {
            ConsensusState.BasePersistedState persistedState = new ConsensusState.BasePersistedState(0L,
                clusterState(0L, 0L, localNode, initialConfiguration, initialConfiguration, 42L));

            Legislator.FutureExecutor futureExecutor = (delay, description, task) ->
                threadPool.schedule(delay, ThreadPool.Names.GENERIC, () -> {
                    synchronized (mutex) {
                        task.run();
                    }
                });

            Legislator.Transport transport = new MockTransport(mockTransportService, mutex);


            legislator = new Legislator(settings, persistedState, transport, masterService,
                ESAllocationTestCase.createAllocationService(), localNode,
                System::nanoTime, futureExecutor, nodeSupplier, clusterApplier, new Random());

            masterService.setClusterStateSupplier(() -> {
                synchronized (mutex) {
                    return legislator.getStateForMasterService();
                }
            });

            masterService.setClusterStatePublisher((clusterChangedEvent, publishListener, ackListener) -> {
                try {
                    synchronized (mutex) {
                        legislator.handleClientValue(clusterChangedEvent.state(), publishListener, ackListener);
                    }
                } catch (Exception e) {
                    assertThat(e.getMessage(), not(containsString("is in progress")));
                    logger.trace(() -> new ParameterizedMessage("[{}] publishing: [{}] failed: {}",
                        localNode.getName(), clusterChangedEvent.source(), e.getMessage()));
                    publishListener.onFailure(new Discovery.FailedToCommitClusterStateException("failure while publishing " +
                        clusterChangedEvent.source(), e));
                }
            });

            masterService.start();

            hookupTransport(mockTransportService, legislator, mutex);

            synchronized (mutex) {
                legislator.start();
            }
        }

        public void close() {
            IOUtils.closeWhileHandlingException(mockTransportService, masterService);
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }


    private class MockClusterApplier implements ClusterApplier {

        final ClusterName clusterName;
        ClusterState lastAppliedClusterState;

        private MockClusterApplier(Settings settings) {
            clusterName = ClusterName.CLUSTER_NAME_SETTING.get(settings);
        }

        @Override
        public void setInitialState(ClusterState initialState) {
            assert lastAppliedClusterState == null;
            assert initialState != null;
            lastAppliedClusterState = initialState;
        }

        @Override
        public void onNewClusterState(String source, Supplier<ClusterState> clusterStateSupplier, ClusterApplyListener listener) {
            lastAppliedClusterState = clusterStateSupplier.get();
            listener.onSuccess(source);
        }

        @Override
        public ClusterState.Builder newClusterStateBuilder() {
            return ClusterState.builder(clusterName);
        }
    }


    class MockTransport implements Legislator.Transport {

        final TransportService transportService;
        final Object mutex;

        MockTransport(TransportService transportService, Object mutex) {
            this.transportService = transportService;
            this.mutex = mutex;
        }

        @Override
        public void sendPublishRequest(DiscoveryNode destination, PublishRequest publishRequest,
                                       TransportResponseHandler<LegislatorPublishResponse> responseHandler) {
            transportService.sendRequest(destination, PUBLISH_ACTION_NAME, publishRequest,
                wrapTransportResponseHandler(responseHandler, mutex));
        }

        @Override
        public void sendHeartbeatRequest(DiscoveryNode destination, HeartbeatRequest heartbeatRequest,
                                         TransportResponseHandler<HeartbeatResponse> responseHandler) {
            transportService.sendRequest(destination, HEARTBEAT_ACTION_NAME, heartbeatRequest,
                wrapTransportResponseHandler(responseHandler, mutex));
        }

        @Override
        public void sendApplyCommit(DiscoveryNode destination, ApplyCommit applyCommit,
                                    TransportResponseHandler<TransportResponse.Empty> responseHandler) {
            transportService.sendRequest(destination, APPLY_COMMIT_ACTION_NAME, applyCommit,
                wrapTransportResponseHandler(responseHandler, mutex));
        }

        @Override
        public void sendSeekJoins(DiscoveryNode destination, SeekJoins seekJoins,
                                  TransportResponseHandler<OfferJoin> responseHandler) {
            transportService.sendRequest(destination, SEEK_JOINS_ACTION_NAME, seekJoins,
                wrapTransportResponseHandler(responseHandler, mutex));
        }

        @Override
        public void sendStartJoin(DiscoveryNode destination, StartJoinRequest startJoinRequest,
                                  TransportResponseHandler<TransportResponse.Empty> responseHandler) {
            transportService.sendRequest(destination, START_JOIN_ACTION_NAME, startJoinRequest,
                wrapTransportResponseHandler(responseHandler, mutex));
        }

        @Override
        public void sendJoin(DiscoveryNode destination, Join join,
                             TransportResponseHandler<TransportResponse.Empty> responseHandler) {
            transportService.sendRequest(destination, JOIN_ACTION_NAME, join,
                wrapTransportResponseHandler(responseHandler, mutex));
        }

        @Override
        public void sendPreJoinHandover(DiscoveryNode destination, PrejoinHandoverRequest prejoinHandoverRequest) {
            transportService.sendRequest(destination, PREJOIN_HANDOVER_ACTION_NAME, prejoinHandoverRequest,
                EmptyTransportResponseHandler.INSTANCE_SAME);
        }

        @Override
        public void sendAbdication(DiscoveryNode destination, AbdicationRequest abdicationRequest) {
            transportService.sendRequest(destination, ABDICATION_ACTION_NAME, abdicationRequest,
                EmptyTransportResponseHandler.INSTANCE_SAME);
        }

        @Override
        public void sendLeaderCheckRequest(DiscoveryNode destination, LeaderCheckRequest leaderCheckRequest,
                                           TransportResponseHandler<Messages.LeaderCheckResponse> responseHandler) {
            transportService.sendRequest(destination, LEADERCHECK_ACTION_NAME, leaderCheckRequest,
                wrapTransportResponseHandler(responseHandler, mutex));
        }
    }


    public static <T extends TransportResponse> TransportResponseHandler<T> wrapTransportResponseHandler(
        TransportResponseHandler<T> transportResponseHandler, Object mutex) {

        return new TransportResponseHandler<T>() {

            @Override
            public T newInstance() {
                return transportResponseHandler.newInstance();
            }

            @Override
            public T read(StreamInput in) throws IOException {
                return transportResponseHandler.read(in);
            }

            @Override
            public void handleResponse(T response) {
                synchronized (mutex) {
                    transportResponseHandler.handleResponse(response);
                }
            }

            @Override
            public void handleException(TransportException exp) {
                synchronized (mutex) {
                    transportResponseHandler.handleException(exp);
                }
            }

            @Override
            public String executor() {
                return ThreadPool.Names.GENERIC;
            }
        };
    }

    public static Legislator.FutureExecutor wrapFutureExecutor(Legislator.FutureExecutor futureExecutor, Object mutex) {
        return (delay, description, task) -> futureExecutor.schedule(delay, description, () -> {
            synchronized (mutex) {
                task.run();
            }
        });
    }

    public static final String PUBLISH_ACTION_NAME = "internal:discovery/zen2/publish/send";
    public static final String APPLY_COMMIT_ACTION_NAME = "internal:discovery/zen2/publish/commit";
    public static final String HEARTBEAT_ACTION_NAME = "internal:discovery/zen2/heartbeat";
    public static final String LEADERCHECK_ACTION_NAME = "internal:discovery/zen2/leadercheck";
    public static final String PREJOIN_HANDOVER_ACTION_NAME = "internal:discovery/zen2/prejoinhandover";
    public static final String ABDICATION_ACTION_NAME = "internal:discovery/zen2/abdication";
    public static final String JOIN_ACTION_NAME = "internal:discovery/zen2/join";
    public static final String SEEK_JOINS_ACTION_NAME = "internal:discovery/zen2/seekjoins";
    public static final String START_JOIN_ACTION_NAME = "internal:discovery/zen2/startjoin";

    public static void hookupTransport(TransportService transportService, Legislator legislator, Object mutex) {

        transportService.registerRequestHandler(PUBLISH_ACTION_NAME, ThreadPool.Names.GENERIC, false, false,
            in -> new PublishRequest(in, legislator.getLocalNode()),
            (request, channel) -> {
                final LegislatorPublishResponse response;
                synchronized (mutex) {
                    response = legislator.handlePublishRequest(request);
                }
                channel.sendResponse(response);
            });

        transportService.registerRequestHandler(APPLY_COMMIT_ACTION_NAME, ThreadPool.Names.GENERIC, false, false,
            ApplyCommit::new,
            (request, channel) -> {
                synchronized (mutex) {
                    legislator.handleApplyCommit(request.sourceNode, request, new ActionListener<Void>() {
                        @Override
                        public void onResponse(Void ignore) {
                            try {
                                channel.sendResponse(TransportResponse.Empty.INSTANCE);
                            } catch (IOException e) {
                                assert false;
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            try {
                                channel.sendResponse(e);
                            } catch (IOException e1) {
                                assert false;
                            }
                        }
                    });
                }
            });

        transportService.registerRequestHandler(HEARTBEAT_ACTION_NAME, ThreadPool.Names.GENERIC, false, false,
            HeartbeatRequest::new,
            (request, channel) -> {
                final HeartbeatResponse response;
                synchronized (mutex) {
                    response = legislator.handleHeartbeatRequest(request.sourceNode, request);
                }
                channel.sendResponse(response);
            });

        transportService.registerRequestHandler(JOIN_ACTION_NAME, ThreadPool.Names.GENERIC, false, false,
            Join::new,
            (request, channel) -> {
                synchronized (mutex) {
                    legislator.handleJoinRequest(request, new MembershipAction.JoinCallback() {
                        @Override
                        public void onSuccess() {
                            try {
                                channel.sendResponse(TransportResponse.Empty.INSTANCE);
                            } catch (IOException e) {
                                assert false;
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            try {
                                channel.sendResponse(e);
                            } catch (IOException e1) {
                                assert false;
                            }
                        }
                    });
                }
            });

        transportService.registerRequestHandler(SEEK_JOINS_ACTION_NAME, ThreadPool.Names.GENERIC, false, false,
            SeekJoins::new,
            (request, channel) -> {
                final OfferJoin offerJoin;
                synchronized (mutex) {
                    offerJoin = legislator.handleSeekJoins(request);
                }
                channel.sendResponse(offerJoin);
            });

        transportService.registerRequestHandler(START_JOIN_ACTION_NAME, ThreadPool.Names.GENERIC, false, false,
            StartJoinRequest::new,
            (request, channel) -> {
                synchronized (mutex) {
                    legislator.handleStartJoin(request);
                }
                channel.sendResponse(TransportResponse.Empty.INSTANCE);
            });

        transportService.registerRequestHandler(LEADERCHECK_ACTION_NAME, ThreadPool.Names.GENERIC, false, false,
            LeaderCheckRequest::new,
            (request, channel) -> {
                final Messages.LeaderCheckResponse leaderCheckResponse;
                synchronized (mutex) {
                    leaderCheckResponse = legislator.handleLeaderCheckRequest(request);
                }
                channel.sendResponse(leaderCheckResponse);
            });

        transportService.registerRequestHandler(PREJOIN_HANDOVER_ACTION_NAME, ThreadPool.Names.GENERIC, false, false,
            PrejoinHandoverRequest::new,
            (request, channel) -> {
                synchronized (mutex) {
                    legislator.handlePreJoinHandover(request);
                }
                channel.sendResponse(TransportResponse.Empty.INSTANCE);
            });

        transportService.registerRequestHandler(ABDICATION_ACTION_NAME, ThreadPool.Names.GENERIC, false, false,
            AbdicationRequest::new,
            (request, channel) -> {
                synchronized (mutex) {
                    legislator.handleAbdication(request);
                }
                channel.sendResponse(TransportResponse.Empty.INSTANCE);
            });
    }


}
