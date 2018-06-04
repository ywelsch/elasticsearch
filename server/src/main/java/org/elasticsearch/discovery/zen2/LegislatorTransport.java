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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.discovery.zen.MembershipAction;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.EmptyTransportResponseHandler;
import org.elasticsearch.transport.TransportConnectionListener;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

public class LegislatorTransport implements Legislator.Transport {

    public static final String PUBLISH_ACTION_NAME = "internal:discovery/zen2/publish/send";
    public static final String APPLY_COMMIT_ACTION_NAME = "internal:discovery/zen2/publish/commit";
    public static final String HEARTBEAT_ACTION_NAME = "internal:discovery/zen2/heartbeat";
    public static final String LEADERCHECK_ACTION_NAME = "internal:discovery/zen2/leadercheck";
    public static final String PREJOIN_HANDOVER_ACTION_NAME = "internal:discovery/zen2/prejoinhandover";
    public static final String ABDICATION_ACTION_NAME = "internal:discovery/zen2/abdication";
    public static final String JOIN_ACTION_NAME = "internal:discovery/zen2/join";
    public static final String SEEK_JOINS_ACTION_NAME = "internal:discovery/zen2/seekjoins";
    public static final String START_JOIN_ACTION_NAME = "internal:discovery/zen2/startjoin";

    private final TransportService transportService;

    public LegislatorTransport(TransportService transportService) {
        this.transportService = transportService;
    }

    public static void registerTransport(TransportService transportService, Legislator legislator) {

        transportService.registerRequestHandler(PUBLISH_ACTION_NAME, ThreadPool.Names.GENERIC, false, false,
            in -> new Messages.PublishRequest(in, transportService.getLocalNode()),
            (request, channel) -> channel.sendResponse(legislator.handlePublishRequest(request)));

        transportService.registerRequestHandler(APPLY_COMMIT_ACTION_NAME, ThreadPool.Names.GENERIC, false, false,
            Messages.ApplyCommit::new,
            (request, channel) -> legislator.handleApplyCommit(request.sourceNode, request, new ActionListener<Void>() {
                @Override
                public void onResponse(Void ignore) {
                    try {
                        channel.sendResponse(TransportResponse.Empty.INSTANCE);
                    } catch (IOException e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    try {
                        channel.sendResponse(e);
                    } catch (Exception inner) {
                        inner.addSuppressed(e);
                        Loggers.getLogger(Legislator.class).warn("failed to send back failure on apply commit request", inner);
                    }
                }
            }));

        transportService.registerRequestHandler(HEARTBEAT_ACTION_NAME, ThreadPool.Names.GENERIC, false, false,
            Messages.HeartbeatRequest::new,
            (request, channel) -> channel.sendResponse(legislator.handleHeartbeatRequest(request.sourceNode, request)));

        transportService.registerRequestHandler(JOIN_ACTION_NAME, ThreadPool.Names.GENERIC, false, false,
            Messages.Join::new,
            (request, channel) -> legislator.handleJoinRequest(request, new MembershipAction.JoinCallback() {
                @Override
                public void onSuccess() {
                    try {
                        channel.sendResponse(TransportResponse.Empty.INSTANCE);
                    } catch (IOException e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    try {
                        channel.sendResponse(e);
                    } catch (Exception inner) {
                        inner.addSuppressed(e);
                        Loggers.getLogger(Legislator.class).warn("failed to send back failure on join request", inner);
                    }
                }
            }));

        transportService.registerRequestHandler(SEEK_JOINS_ACTION_NAME, ThreadPool.Names.GENERIC, false, false,
            Messages.SeekJoins::new,
            (request, channel) -> channel.sendResponse(legislator.handleSeekJoins(request)));

        transportService.registerRequestHandler(START_JOIN_ACTION_NAME, ThreadPool.Names.GENERIC, false, false,
            Messages.StartJoinRequest::new,
            (request, channel) -> {
                legislator.handleStartJoin(request);
                channel.sendResponse(TransportResponse.Empty.INSTANCE);
            });

        transportService.registerRequestHandler(LEADERCHECK_ACTION_NAME, ThreadPool.Names.GENERIC, false, false,
            Messages.LeaderCheckRequest::new,
            (request, channel) -> channel.sendResponse(legislator.handleLeaderCheckRequest(request)));

        transportService.registerRequestHandler(PREJOIN_HANDOVER_ACTION_NAME, ThreadPool.Names.GENERIC, false, false,
            Messages.PrejoinHandoverRequest::new,
            (request, channel) -> {
                legislator.handlePreJoinHandover(request);
                channel.sendResponse(TransportResponse.Empty.INSTANCE);
            });

        transportService.registerRequestHandler(ABDICATION_ACTION_NAME, ThreadPool.Names.GENERIC, false, false,
            Messages.AbdicationRequest::new,
            (request, channel) -> {
                legislator.handleAbdication(request);
                channel.sendResponse(TransportResponse.Empty.INSTANCE);
            });

        transportService.addConnectionListener(new TransportConnectionListener() {
            @Override
            public void onNodeDisconnected(DiscoveryNode node) {
                legislator.handleDisconnectedNode(node);
            }
        });
    }

    @Override
    public void sendPublishRequest(DiscoveryNode destination, Messages.PublishRequest publishRequest,
                                   TransportResponseHandler<Messages.LegislatorPublishResponse> responseHandler) {
        transportService.sendRequest(destination, PUBLISH_ACTION_NAME, publishRequest, responseHandler);
    }

    @Override
    public void sendHeartbeatRequest(DiscoveryNode destination, Messages.HeartbeatRequest heartbeatRequest,
                                     TransportResponseHandler<Messages.HeartbeatResponse> responseHandler) {
        transportService.sendRequest(destination, HEARTBEAT_ACTION_NAME, heartbeatRequest, responseHandler);
    }

    @Override
    public void sendApplyCommit(DiscoveryNode destination, Messages.ApplyCommit applyCommit,
                                TransportResponseHandler<TransportResponse.Empty> responseHandler) {
        transportService.sendRequest(destination, APPLY_COMMIT_ACTION_NAME, applyCommit, responseHandler);
    }

    @Override
    public void sendSeekJoins(DiscoveryNode destination, Messages.SeekJoins seekJoins,
                              TransportResponseHandler<Messages.OfferJoin> responseHandler) {
        transportService.sendRequest(destination, SEEK_JOINS_ACTION_NAME, seekJoins, responseHandler);
    }

    @Override
    public void sendStartJoin(DiscoveryNode destination, Messages.StartJoinRequest startJoinRequest,
                              TransportResponseHandler<TransportResponse.Empty> responseHandler) {
        transportService.sendRequest(destination, START_JOIN_ACTION_NAME, startJoinRequest, responseHandler);
    }

    @Override
    public void sendJoin(DiscoveryNode destination, Messages.Join join,
                         TransportResponseHandler<TransportResponse.Empty> responseHandler) {
        transportService.sendRequest(destination, JOIN_ACTION_NAME, join, responseHandler);
    }

    @Override
    public void sendPreJoinHandover(DiscoveryNode destination, Messages.PrejoinHandoverRequest prejoinHandoverRequest) {
        transportService.sendRequest(destination, PREJOIN_HANDOVER_ACTION_NAME, prejoinHandoverRequest,
            EmptyTransportResponseHandler.INSTANCE_SAME);
    }

    @Override
    public void sendAbdication(DiscoveryNode destination, Messages.AbdicationRequest abdicationRequest) {
        transportService.sendRequest(destination, ABDICATION_ACTION_NAME, abdicationRequest,
            EmptyTransportResponseHandler.INSTANCE_SAME);
    }

    @Override
    public void sendLeaderCheckRequest(DiscoveryNode destination, Messages.LeaderCheckRequest leaderCheckRequest,
                                       TransportResponseHandler<Messages.LeaderCheckResponse> responseHandler) {
        transportService.sendRequest(destination, LEADERCHECK_ACTION_NAME, leaderCheckRequest, responseHandler);
    }

    @Override
    public DiscoveryNode getLocalNode() {
        return transportService.getLocalNode();
    }

    @Override
    public void connectToNode(DiscoveryNode node) {
        transportService.connectToNode(node);
    }

}
