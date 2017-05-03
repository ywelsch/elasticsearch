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

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterApplier;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.discovery.DiscoveryStats;
import org.elasticsearch.discovery.zen.MembershipAction.JoinRequest;
import org.elasticsearch.discovery.zen.PublishClusterStateAction.IncomingClusterStateListener;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.EmptyTransportResponseHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.elasticsearch.discovery.zen.DiscoPhase.Become_Follower;
import static org.elasticsearch.discovery.zen.DiscoPhase.Follower;
import static org.elasticsearch.discovery.zen.DiscoPhase.Master;
import static org.elasticsearch.discovery.zen.DiscoPhase.Pinging;
import static org.elasticsearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;

public class ZenDiscovery2 extends AbstractLifecycleComponent implements Discovery, PingContextProvider, IncomingClusterStateListener {

    private final TransportService transportService;
    private final MasterService masterService;
    private final ClusterName clusterName;
    private final DiscoverySettings discoverySettings;
    protected final ZenPing zenPing; // protected to allow tests access
    private final MasterFaultDetection masterFD;
    private final NodesFaultDetection nodesFD;
    private final PublishClusterStateAction publishClusterState;
    private final MembershipAction membership;
    private final ThreadPool threadPool;

    private final TimeValue pingTimeout;
    private final TimeValue joinTimeout;

    /** how many retry attempts to perform if join request failed with an retriable error */
    private final int joinRetryAttempts;
    /** how long to wait before performing another join attempt after a join request failed with an retriable error */
    private final TimeValue joinRetryDelay;

    /** how many pings from *another* master to tolerate before forcing a rejoin on other or local master */
    private final int maxPingsFromAnotherMaster;

    // a flag that should be used only for testing
    private final boolean sendLeaveRequest;

    private final ElectMasterService electMaster;

    private final boolean masterElectionIgnoreNonMasters;
    private final TimeValue masterElectionWaitForJoinsTimeout;

    private final JoinTaskExecutor joinTaskExecutor;
    private final NodeRemovalClusterStateTaskExecutor nodeRemovalExecutor;

    private final ClusterApplier clusterApplier;
    private final AtomicReference<ZenState> state;
    private final Object stateMutex = new Object();
    private final AllocationService allocationService;

    ScheduledFuture timeoutFuture = null; // cancelled and reset whenever we move away from Become_Follower or Become_Master state (use cancel(false))
    AtomicLong currentPingingRound = new AtomicLong(); // pinging round, initially 0
    ClusterState lastCommittedState;
    private final Map<JoinRequest, MembershipAction.JoinCallback> joinRequests = new HashMap<>();
    private final Map<String, ClusterState> pendingApplyQueue = new ConcurrentHashMap<>(); // does not use synchronization



    public ZenDiscovery2(Settings settings, ThreadPool threadPool, TransportService transportService,
                         NamedWriteableRegistry namedWriteableRegistry, MasterService masterService, ClusterApplier clusterApplier,
                         ClusterSettings clusterSettings, UnicastHostsProvider hostsProvider, AllocationService allocationService) {
        super(settings);
        this.masterService = masterService;
        this.clusterApplier = clusterApplier;
        this.transportService = transportService;
        this.discoverySettings = new DiscoverySettings(settings, clusterSettings);
        this.zenPing = newZenPing(settings, threadPool, transportService, hostsProvider);
        this.electMaster = new ElectMasterService(settings);
        this.pingTimeout = ZenDiscovery.PING_TIMEOUT_SETTING.get(settings);
        this.joinTimeout = ZenDiscovery.JOIN_TIMEOUT_SETTING.get(settings);
        this.joinRetryAttempts = ZenDiscovery.JOIN_RETRY_ATTEMPTS_SETTING.get(settings);
        this.joinRetryDelay = ZenDiscovery.JOIN_RETRY_DELAY_SETTING.get(settings);
        this.maxPingsFromAnotherMaster = ZenDiscovery.MAX_PINGS_FROM_ANOTHER_MASTER_SETTING.get(settings);
        this.sendLeaveRequest = ZenDiscovery.SEND_LEAVE_REQUEST_SETTING.get(settings);
        this.threadPool = threadPool;
        this.clusterName = ClusterName.CLUSTER_NAME_SETTING.get(settings);
        this.state = new AtomicReference<>();

        this.masterElectionIgnoreNonMasters = ZenDiscovery.MASTER_ELECTION_IGNORE_NON_MASTER_PINGS_SETTING.get(settings);
        this.masterElectionWaitForJoinsTimeout = ZenDiscovery.MASTER_ELECTION_WAIT_FOR_JOINS_TIMEOUT_SETTING.get(settings);

        logger.debug("using ping_timeout [{}], join.timeout [{}], master_election.ignore_non_master [{}]",
                this.pingTimeout, joinTimeout, masterElectionIgnoreNonMasters);

        clusterSettings.addSettingsUpdateConsumer(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING,
            this::handleMinimumMasterNodesChanged, (value) -> {
                final ClusterState clusterState = this.clusterState();
                int masterNodes = clusterState.nodes().getMasterNodes().size();
                // the purpose of this validation is to make sure that the master doesn't step down
                // due to a change in master nodes, which also means that there is no way to revert
                // an accidental change. Since we validate using the current cluster state (and
                // not the one from which the settings come from) we have to be careful and only
                // validate if the local node is already a master. Doing so all the time causes
                // subtle issues. For example, a node that joins a cluster has no nodes in its
                // current cluster state. When it receives a cluster state from the master with
                // a dynamic minimum master nodes setting int it, we must make sure we don't reject
                // it.

                if (clusterState.nodes().isLocalNodeElectedMaster() && value > masterNodes) {
                    throw new IllegalArgumentException("cannot set "
                        + ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey() + " to more than the current" +
                        " master nodes count [" + masterNodes + "]");
                }
        });

        this.masterFD = new MasterFaultDetection(settings, threadPool, transportService, this::clusterState, masterService, clusterName);
        this.masterFD.addListener(new MasterNodeFailureListener());
        this.nodesFD = new NodesFaultDetection(settings, threadPool, transportService, clusterName);
        this.nodesFD.addListener(new NodeFaultDetectionListener());

        this.publishClusterState =
                new PublishClusterStateAction(
                        settings,
                        transportService,
                        namedWriteableRegistry,
                        this,
                        discoverySettings);
        this.membership = new MembershipAction(settings, transportService, new MembershipListener());

        this.allocationService = allocationService;
        this.joinTaskExecutor = new JoinTaskExecutor();
        this.nodeRemovalExecutor = new NodeRemovalClusterStateTaskExecutor(allocationService, electMaster, this::submitRejoin, logger);

        masterService.setClusterStateSupplier(this::clusterState);

        transportService.registerRequestHandler(
            ZenDiscovery.DISCOVERY_REJOIN_ACTION_NAME, RejoinClusterRequest::new, ThreadPool.Names.SAME, new RejoinClusterRequestHandler());
    }

    // protected to allow overriding in tests
    protected ZenPing newZenPing(Settings settings, ThreadPool threadPool, TransportService transportService,
                                 UnicastHostsProvider hostsProvider) {
        return new UnicastZenPing(settings, threadPool, transportService, hostsProvider, this);
    }

    @Override
    protected void doStart() {
        DiscoveryNode localNode = transportService.getLocalNode();
        assert localNode != null;
        synchronized (stateMutex) {
            // set initial state
            assert state.get() == null;
            assert localNode != null;
            ClusterState initialState = ClusterState.builder(clusterName)
                .blocks(ClusterBlocks.builder()
                    .addGlobalBlock(STATE_NOT_RECOVERED_BLOCK)
                    .addGlobalBlock(discoverySettings.getNoMasterBlock()))
                .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()))
                .build();
            clusterApplier.setInitialState(initialState);
            lastCommittedState = initialState;
            state.set(new ZenState(Pinging, ZenState.UNKNOWN_TERM, ZenState.UNKNOWN_TERM, initialState));
            nodesFD.setLocalNode(localNode);
        }
        zenPing.start();
    }

    @Override
    public void startInitialJoin() {
        synchronized(stateMutex) {
            if (currentPingingRound.get() == 0L) {
                startPinging();
            }
        }
    }

    private void startPinging() {
        assert Thread.holdsLock(stateMutex);
        assert state.get().getDiscoPhase() == Pinging : "Pinging started in non-pinging state: " + state.get().getDiscoPhase();
        if (lifecycle.stoppedOrClosed()) {
            return;
        }
        long pingingRound = currentPingingRound.incrementAndGet();
        logger.debug("starting pinging round [{}], node term [{}]", pingingRound, state.get().getNodeTerm());
        threadPool.generic().execute(new AbstractRunnable() {
            @Override
            public void onRejection(Exception e) {
                assert ZenDiscovery2.this.lifecycle.stoppedOrClosed();
                // node is shutting down
            }

            @Override
            public void onFailure(Exception e) {
                if (e instanceof EsRejectedExecutionException) {
                    assert ZenDiscovery2.this.lifecycle.stoppedOrClosed();
                } else {
                    assert false : e.toString();
                }
            }

            @Override
            protected void doRun() throws Exception {
                zenPing.ping(responses -> handlePingResponses(responses.toList(), pingingRound), pingTimeout);
            }
        });
    }

    public void handlePingResponses(List<ZenPing.PingResponse> pingResponses, long pingingRound) {
        assert genericThread();
        if (state.get().getDiscoPhase() != Pinging || pingingRound != currentPingingRound.get()) {
            // ignore
            return;
        }
        synchronized(stateMutex) {
            ZenState zenState = state.get();
            if (zenState.getDiscoPhase() != Pinging || pingingRound != currentPingingRound.get()) {
                // ignore
                return;
            }

            if (logger.isTraceEnabled()) {
                StringBuilder sb = new StringBuilder();
                if (pingResponses.size() == 0) {
                    sb.append(" {none}");
                } else {
                    for (ZenPing.PingResponse pingResponse : pingResponses) {
                        sb.append("\n\t--> ").append(pingResponse);
                    }
                }
                logger.trace("full ping responses:{}", sb);
            }

            final DiscoveryNode localNode = transportService.getLocalNode();

            pingResponses.add(new ZenPing.PingResponse(localNode, zenState.getClusterState().nodes().getMasterNode(), zenState));

            // maybe filter responses from non-master nodes (similar to ZenDiscovery)
            Optional<DiscoveryNode> activeMaster = pingResponses.stream()
                .filter(pr -> pr.master() != null)
                .filter(pr -> pr.master().equals(localNode) == false) // we know we are not master
                .filter(pr -> pr.getNodeTerm() == ZenState.UNKNOWN_TERM || pr.getNodeTerm() >= zenState.getNodeTerm())
                .sorted(Comparator.comparing(ZenPing.PingResponse::getNodeTerm).reversed()) // prefer higher node term
                .map(ZenPing.PingResponse::master)
                .findFirst();

            final DiscoPhase decision; // compute decision whether to continue pinging, or become master / follower
            final long electionTerm; // compute term to use if becoming master or follower
            final DiscoveryNode masterNode; // master node to join if becoming follower

            if (activeMaster.isPresent()) {
                decision = DiscoPhase.Become_Follower;
                electionTerm = 0L; // don't send a term, this is a non-voting join
                masterNode = activeMaster.get();
            } else {
                List<ZenPing.PingResponse> masterCandidates = pingResponses.stream()
                    .filter(pr -> pr.node().isMasterNode())
                    .collect(Collectors.toList());
                if (masterCandidates.isEmpty() || masterCandidates.size() < electMaster.minimumMasterNodes()) {
                    decision = DiscoPhase.Pinging;
                    electionTerm = 0L;
                    masterNode = null;
                } else {
                    ZenPing.PingResponse bestCandidate = masterCandidates.stream()
                        .sorted(Comparator.comparing(ZenPing.PingResponse::getClusterStateTerm).reversed()
                            .thenComparing(Comparator.comparing(ZenPing.PingResponse::getClusterStateVersion).reversed())
                            .thenComparing(pr -> pr.node().getId()))
                        .findFirst()
                        .get();
                    masterNode = bestCandidate.node();
                    if (masterNode.equals(localNode)) {
                        decision = DiscoPhase.Become_Master;
                    } else {
                        decision = DiscoPhase.Become_Follower;
                    }
                    electionTerm = masterCandidates.stream()
                        .map(pr -> {
                            if (pr.getDiscoPhase() == DiscoPhase.Become_Follower || pr.getDiscoPhase() == DiscoPhase.Become_Master) {
                                return pr.getNodeTerm(); // already a speculative term
                            } else {
                                return pr.getNodeTerm() + 1;
                            }
                        })
                        .reduce(Math::max)
                        .get();
                }
            }

            if (decision == DiscoPhase.Become_Master) {
                logger.debug("decided to become master with term [{}]", electionTerm);
                persistTerm(electionTerm); // persist term first
                state.getAndUpdate(st -> st.withPhaseAndNodeTerm(DiscoPhase.Become_Master, electionTerm));
                // schedule timeout to fail if we have not become master during that time
                timeoutFuture = threadPool.schedule(masterElectionWaitForJoinsTimeout, ThreadPool.Names.GENERIC, () -> timeoutTriggered(DiscoPhase.Become_Master, electionTerm)); // do the scheduling outside synchronized block?
                handleJoinRequest(new JoinRequest(clusterState().nodes().getLocalNode(), electionTerm), new MembershipAction.JoinCallback() {
                    @Override
                    public void onSuccess() {
                        // ignore
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.debug("self-join failed");
                        synchronized (stateMutex) {
                            moveToPinging(electionTerm);
                        }
                    }
                }); // add fake joinRequest for local node
            } else if (decision == DiscoPhase.Become_Follower) {
                logger.debug("decided to become follower with term [{}]", electionTerm);
                persistTerm(electionTerm); // persist term first
                state.getAndUpdate(st -> st.withPhaseAndNodeTerm(DiscoPhase.Become_Follower, electionTerm));
                // TODO: should this also fail all joinrequests? maybe introduce method "moveToBecomeFollower"
                try {
                    // first, make sure we can connect to the master
                    transportService.connectToNode(masterNode);
                    membership.sendJoinRequest(masterNode, zenState.getClusterState().getNodes().getLocalNode(), electionTerm,
                        new EmptyTransportResponseHandler(ThreadPool.Names.GENERIC) {
                            @Override
                            public void handleException(TransportException exp) {
                                joinFailed(exp, electionTerm);
                            }
                        });
                } catch (Exception e) {
                    logger.warn("join failed", e);
                    moveToPinging(electionTerm);
                    return;
                }
                // schedule timeout to fail if we have not become follower during that time
                timeoutFuture = threadPool.schedule(joinTimeout, ThreadPool.Names.GENERIC, () -> timeoutTriggered(DiscoPhase.Become_Follower, electionTerm)); // do the scheduling outside synchronized block?
            } else if (decision == Pinging) {
                logger.debug("no decision reached, continuing pinging");
                startPinging();
            }
        }
    }

    public void joinFailed(Exception e, long term) {
        assert genericThread();
        // Become_Follower increments term, so we can check if the response matches the request using the term
        ZenState zenStateSnapshot = state.get();
        if (zenStateSnapshot.getDiscoPhase() == Become_Follower && zenStateSnapshot.getNodeTerm() == term) {
            synchronized(stateMutex) {
                ZenState zenState = state.get();
                if (zenState.getDiscoPhase() == Become_Follower && zenState.getNodeTerm() == term) {
                    logger.warn("join failed", e);
                    moveToPinging(term);
                } else {
                    logger.warn("join failed but ignored", e);
                    // just ignore, it's an event from an older term or discoPhase has since changed
                }
            }
        }
    }

    public void timeoutTriggered(DiscoPhase expectedDiscoPhase, long term) {
        assert genericThread();
        // Become_Master increments term, so we can check if the response matches the request using the term
        ZenState zenStateSnapshot = state.get();
        if (zenStateSnapshot.getDiscoPhase() == expectedDiscoPhase && zenStateSnapshot.getNodeTerm() == term) {
            synchronized(stateMutex) {
                ZenState zenState = state.get();
                if (zenState.getDiscoPhase() == expectedDiscoPhase && zenState.getNodeTerm() == term) {
                    moveToPinging(term);
                } else {
                    // just ignore, it's an event from an older term or discoPhase has since changed
                }
            }
        }
    }

    private void moveToPinging() {
        moveToPinging(state.get().getNodeTerm());
    }

    private void moveToPinging(long term) {
        assert Thread.holdsLock(stateMutex);
        if (term < state.get().getNodeTerm()) {
            // do nothing
            logger.debug("move to pinging called, but with lower term [{}] than current node term [{}]", term, state.get().getNodeTerm());
            return;
        }
        ZenState previousZenState = state.get();
        assert term >= previousZenState.getNodeTerm() : "Moving to pinging with term " + term + " but current term is already " + previousZenState.getNodeTerm();
        logger.debug("move to pinging called with term [{}], current node term [{}]", term, previousZenState.getNodeTerm());

        final long newTerm;
        if (term > previousZenState.getNodeTerm()) {
            newTerm = term;
            persistTerm(newTerm);
        } else {
            newTerm = previousZenState.getNodeTerm();
        }
        state.getAndUpdate(st -> new ZenState(DiscoPhase.Pinging, newTerm, st.getClusterStateTerm(),
            stateWithBlocksApplied(Pinging, st.getClusterState(), discoverySettings.getNoMasterBlock())));

        nodesFD.stop();
        masterFD.stop("moving to pinging");
        if (previousZenState.getDiscoPhase() != Pinging) {
            failJoinRequests();
        }
        pendingApplyQueue.clear();
        FutureUtils.cancel(timeoutFuture);
        timeoutFuture = null;
        if (previousZenState.getDiscoPhase() != Pinging || term > previousZenState.getNodeTerm()) {
            startPinging();

            // check if no_master_block is set, if no, set it
            // between setting the blocks on a state and another (better) state being applied?
            // (maybe only an issue if we don't take version into account)
            // take last state sent to applier and add no_master_block instead of last published one?
            // this requires us to store the last applier state
            // we could as well just ignore applier messages in case the node has reverted back to pinging?
            lastCommittedState = stateWithBlocksApplied(Pinging, lastCommittedState, discoverySettings.getNoMasterBlock());
            clusterApplier.onNewClusterState("back-to-pinging", () -> lastCommittedState, (source, e) -> {});
        }
    }

    public static ClusterState stateWithBlocksApplied(DiscoPhase discoPhase, ClusterState clusterState, ClusterBlock noMasterBlock) {
        if (discoPhase == null || discoPhase == Follower || discoPhase == Master || clusterState.nodes().getMasterNodeId() == null) {
            return clusterState;
        } else {
            ClusterBlocks clusterBlocks = ClusterBlocks.builder().blocks(clusterState.blocks())
                .addGlobalBlock(noMasterBlock)
                .build();

            DiscoveryNodes discoveryNodes = new DiscoveryNodes.Builder(clusterState.nodes()).masterNodeId(null).build();
            return ClusterState.builder(clusterState)
                .blocks(clusterBlocks)
                .nodes(discoveryNodes)
                .build();
        }
    }

    private void moveToFollower(long term, DiscoveryNode masterNode) {
        assert Thread.holdsLock(stateMutex);
        assert term >= state.get().getNodeTerm();
        ZenState previousZenState = state.get();
        logger.debug("move to follower called with term [{}], current node term [{}]", term, previousZenState.getNodeTerm());

        final long newTerm;
        if (term > previousZenState.getNodeTerm()) {
            newTerm = term;
            persistTerm(term);
        } else {
            newTerm = previousZenState.getNodeTerm();
        }
        state.getAndUpdate(st -> st.withPhaseAndNodeTerm(DiscoPhase.Follower, newTerm));

        failJoinRequests();
        FutureUtils.cancel(timeoutFuture);
        timeoutFuture = null;
        if (previousZenState.getDiscoPhase() != DiscoPhase.Follower) {
            nodesFD.stop();
        }
        // start master fault detection if needed
        if (masterFD.masterNode() == null || !masterFD.masterNode().equals(masterNode)) {
            masterFD.restart(masterNode, "new cluster state received and we are monitoring the wrong master [" + masterFD.masterNode() + "]");
        }
    }

    // protected so that it can be overridden by tests
    protected void persistTerm(long term) {
        assert Thread.holdsLock(stateMutex);
        // this method should probably also specify what exception is thrown when persistence fails
    }

    private void moveToMaster() {
        assert Thread.holdsLock(stateMutex);
        assert state.get().getDiscoPhase() == DiscoPhase.Become_Master;
        assert joinRequests.isEmpty();
        logger.debug("move to master called for term [{}]", state.get().getNodeTerm());
        // term is not updated here, no need to persist it
        state.getAndUpdate(st -> st.withPhaseAndNodeTerm(DiscoPhase.Master, st.getNodeTerm())); // should nodesFD be started here or in handlePublish...? Maybe even call this method in handlePublish..., but we also need to account for the case where the submitted cluster state update task to become master fails as well as the become_master timeout trigger
        pendingApplyQueue.clear();
        FutureUtils.cancel(timeoutFuture);
        timeoutFuture = null;
    }

    private void failJoinRequests() {
        assert Thread.holdsLock(stateMutex);
        joinRequests.entrySet().forEach(jr -> {
            logger.info("failing join request {}, phase is [{}]", jr.getKey(), state.get().getDiscoPhase());
            jr.getValue().onFailure(new IllegalStateException("failed join request because moved to phase " + state.get().getDiscoPhase()));
        });
        joinRequests.clear();
    }

    @Override
    public void onPing(DiscoveryNode node, long term) {
        assert genericThread();
        // first check outside mutex, as this block of code executes very often
        ZenState zenStateSnapshot = state.get();
        if (zenStateSnapshot.getDiscoPhase() != Pinging && term > zenStateSnapshot.getNodeTerm()) {
            synchronized(stateMutex) {
                ZenState zenState = state.get();
                if (zenState.getDiscoPhase() != Pinging && term > zenState.getNodeTerm()) {
                    logger.debug("learned about higher term [{}] than we currently have [{}], fall back to pinging", term, zenState.getNodeTerm());
                    moveToPinging(term); // If state is Become_Master, we should not use term, otherwise we cannot vote
                }
            }
        }
    }

    private final Object joinMutex = new Object();

    public void handleJoinRequest(JoinRequest joinRequest, MembershipAction.JoinCallback joinCallback) {
        assert genericThread();
        try {
            synchronized (joinMutex) {
                ZenState zenState = state.get();
                if (zenState.getDiscoPhase() == Master && joinRequest.term <= zenState.getNodeTerm()) {
                    // consider as non-voting join request
                    // no need to do this under stateMutex, if we're wrong, the task will fail anyhow
                    // submit this with "runOnlyOnMaster = true"
                    logger.debug("handle non-voting join request [{}]", joinRequest);
                    masterService.submitStateUpdateTask("zen-disco-node-join",
                        joinRequest, ClusterStateTaskConfig.build(Priority.URGENT),
                        joinTaskExecutor, new NodeJoinController.JoinTaskListener(joinCallback, logger));
                    return;
                }
            }
            if (joinRequest.voting() == false || joinRequest.term >= state.get().getNodeTerm()) {
                synchronized (stateMutex) {
                    ZenState zenState = state.get();
                    if (joinRequest.voting() && joinRequest.term < zenState.getNodeTerm()) {
                        throw new RuntimeException("term too low");
                    } else if (joinRequest.term > zenState.getNodeTerm() && zenState.getDiscoPhase() != Pinging) {
                        // if we're in Pinging mode, we might still be unaware that we will become master
                        // but if we're becoming master, we should have the correct speculative term
                        logger.debug("handle join request with higher term although we're not pinging [{}] [{}:{}]", joinRequest, zenState.getDiscoPhase(), zenState.getNodeTerm());
                        joinRequests.put(joinRequest, joinCallback); // add it to be failed in moveToPinging
                        moveToPinging(joinRequest.term);
                    } else if (zenState.getDiscoPhase() == Master) {
                        // consider as non-voting join request
                        // submit this with "runOnlyOnMaster = true"
                        logger.debug("handle non-voting join request [{}]", joinRequest);
                        masterService.submitStateUpdateTask("zen-disco-node-join",
                            joinRequest, ClusterStateTaskConfig.build(Priority.URGENT),
                            joinTaskExecutor, new NodeJoinController.JoinTaskListener(joinCallback, logger));
                    } else {
                        // collect all join requests with joinRequest.term == 0 or joinRequest.term >= nodeTerm.
                        logger.debug("handle voting join request [{}]", joinRequest);
                        joinRequests.put(joinRequest, joinCallback);

                        if (zenState.getDiscoPhase() == Pinging) {
                            // TODO: schedule timeout for these incoming join requests in we're in Pinging mode (this allows them not to linger indefinitely)
                        }

                        if (zenState.getDiscoPhase() == DiscoPhase.Become_Master) {
                            List<Map.Entry<JoinRequest, MembershipAction.JoinCallback>> votingNodesWithTerm = joinRequests.entrySet().stream().filter(jr -> jr.getKey().voting() && jr.getKey().getTerm() == zenState.getNodeTerm()).collect(Collectors.toList());
                            if (votingNodesWithTerm.size() >= electMaster.minimumMasterNodes()) {
                                synchronized (joinMutex) {
                                    // we could submit an update task that contains nodes that joined + the term under which this is happening
                                    // joinRequests should contain a vote from ourself (otherwise we would not be in state Become_Master

                                    // easier: send state update that contains term and that gets joinRequests and term etc. from us (reads under state mutex).
                                    // This means that we won't have to explicitly pass joiningNodes etc. as parameter.

                                    //                        masterService.submitStateUpdateTasks(joiningNodesWithTerm); // also add non-master nodes

                                    // what should we do if this fails? this should still reach the publish phase
                                    // maybe to be extra safe, we should add a handler that moves us back to Pinging if this fails


                                    long masterTerm = zenState.getNodeTerm();

                                    List<Map.Entry<JoinRequest, MembershipAction.JoinCallback>> joiningNodes = joinRequests.entrySet().stream().filter(jr -> jr.getKey().voting() == false || jr.getKey().getTerm() == zenState.getNodeTerm()).collect(Collectors.toList());
                                    List<Map.Entry<JoinRequest, MembershipAction.JoinCallback>> joiningNodesWithWrongTerm = joinRequests.entrySet().stream().filter(jr -> jr.getKey().voting() && jr.getKey().getTerm() != zenState.getNodeTerm()).collect(Collectors.toList());

                                    joinRequests.clear();
                                    moveToMaster(); // by using this approach, we can simply check in publish method whether we are master and whether term matches

                                    // fail all joins that are voting and that have not same term as us (maybe as part of CS update)
                                    joiningNodesWithWrongTerm.forEach(jr -> {
                                        logger.info("failing joining node {}, expected term is [{}]", jr.getKey(), masterTerm);
                                        jr.getValue().onFailure(new RuntimeException("expected join with term " + masterTerm + " but was " + jr.getKey().getTerm()));
                                    });

                                    Map<JoinRequest, ClusterStateTaskListener> tasks = joiningNodes.stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> new ClusterStateTaskListener() {

                                        @Override
                                        public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                                            entry.getValue().onSuccess();
                                        }

                                        @Override
                                        public void onFailure(String source, Exception e) {
                                            entry.getValue().onFailure(e);
                                            // TODO: moveToPinging(masterTerm);
                                        }
                                    }));
                                    masterService.submitStateUpdateTasks("Become Master", tasks, ClusterStateTaskConfig.build(Priority.URGENT),
                                        new ClusterStateTaskExecutor<JoinRequest>() {
                                            @Override
                                            public ClusterTasksResult<JoinRequest> execute(ClusterState currentState, List<JoinRequest> tasks) throws Exception {
                                                final ClusterTasksResult.Builder<JoinRequest> results = ClusterTasksResult.builder();
                                                ClusterState.Builder newState;

                                                //TODO: add a term check here? i.e. that this is executing for the right term
                                                if (currentState.nodes().getMasterNode() == null) {
                                                    newState = becomeMasterAndTrimConflictingNodes(currentState, tasks.stream().map(JoinRequest::getDiscoveryNode).collect(Collectors.toList()));
                                                    DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(newState.nodes());

                                                    boolean nodesChanged = false;

                                                    Version minNodeVersion = Version.CURRENT;
                                                    for (final JoinRequest joinRequest : tasks) {
                                                        DiscoveryNode node = joinRequest.getDiscoveryNode();
                                                        minNodeVersion = Version.min(minNodeVersion, node.getVersion());
                                                        if (currentState.nodes().nodeExists(node)) {
                                                            logger.debug("received a join request for an existing node [{}]", node);
                                                        } else {
                                                            try {
                                                                nodesBuilder.add(node);
                                                                nodesChanged = true;
                                                            } catch (IllegalArgumentException e) {
                                                                results.failure(joinRequest, e);
                                                                continue;
                                                            }
                                                        }
                                                        results.success(joinRequest);
                                                    }

                                                    // we do this validation quite late to prevent race conditions between nodes joining and importing dangling indices
                                                    // we have to reject nodes that don't support all indices we have in this cluster
                                                    MembershipAction.ensureIndexCompatibility(minNodeVersion, currentState.getMetaData());
                                                    if (nodesChanged) {
                                                        newState.nodes(nodesBuilder);
                                                        return results.build(allocationService.reroute(newState.build(), "node_join"));
                                                    } else {
                                                        // we must return a new cluster state instance to force publishing. This is important
                                                        // for the joining node to finalize its join and set us as a master
                                                        return results.build(newState.build());
                                                    }
                                                } else {
                                                    // fail all, there is already a master
                                                    throw new NotMasterException("Node [" + currentState.nodes().getLocalNode() + "] not master for join request");
                                                }
                                            }

                                            private ClusterState.Builder becomeMasterAndTrimConflictingNodes(ClusterState currentState, List<DiscoveryNode> joiningNodes) {
                                                assert currentState.nodes().getMasterNodeId() == null : currentState;
                                                DiscoveryNodes currentNodes = currentState.nodes();
                                                DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(currentNodes);
                                                nodesBuilder.masterNodeId(currentState.nodes().getLocalNodeId());

                                                for (final DiscoveryNode joiningNode : joiningNodes) {
                                                    final DiscoveryNode nodeWithSameId = nodesBuilder.get(joiningNode.getId());
                                                    if (nodeWithSameId != null && nodeWithSameId.equals(joiningNode) == false) {
                                                        logger.debug("removing existing node [{}], which conflicts with incoming join from [{}]", nodeWithSameId, joiningNode);
                                                        nodesBuilder.remove(nodeWithSameId.getId());
                                                    }
                                                    final DiscoveryNode nodeWithSameAddress = currentNodes.findByAddress(joiningNode.getAddress());
                                                    if (nodeWithSameAddress != null && nodeWithSameAddress.equals(joiningNode) == false) {
                                                        logger.debug("removing existing node [{}], which conflicts with incoming join from [{}]", nodeWithSameAddress,
                                                            joiningNode);
                                                        nodesBuilder.remove(nodeWithSameAddress.getId());
                                                    }
                                                }


                                                // now trim any left over dead nodes - either left there when the previous master stepped down
                                                // or removed by us above
                                                ClusterState tmpState = ClusterState.builder(currentState).nodes(nodesBuilder).blocks(ClusterBlocks.builder()
                                                    .blocks(currentState.blocks())
                                                    .removeGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID)).build();
                                                return ClusterState.builder(allocationService.deassociateDeadNodes(tmpState, false,
                                                    "removed dead nodes on election"));
                                            }

                                            public boolean runOnlyOnMaster() {
                                                return false;
                                            }
                                        });
                                }
                            }
                        }
                    }
                }
            } else {
                throw new RuntimeException("term too low");
            }
        } catch (Exception e) {
            joinCallback.onFailure(e);
        }
    }

    class JoinTaskExecutor implements ClusterStateTaskExecutor<JoinRequest> {

        @Override
        public ClusterTasksResult<JoinRequest> execute(ClusterState currentState, List<JoinRequest> joiningNodes) throws Exception {
            final ClusterTasksResult.Builder<JoinRequest> results = ClusterTasksResult.builder();

            final DiscoveryNodes currentNodes = currentState.nodes();
            boolean nodesChanged = false;
            ClusterState.Builder newState = ClusterState.builder(currentState);;

            // distinguish whether we want to become master or whether we are master already
            if (currentNodes.isLocalNodeElectedMaster()) {
                DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(newState.nodes());

                assert nodesBuilder.isLocalNodeElectedMaster();

                Version minNodeVersion = Version.CURRENT;
                // processing any joins
                for (final JoinRequest joinRequest : joiningNodes) {
                    DiscoveryNode node = joinRequest.getDiscoveryNode();
                    minNodeVersion = Version.min(minNodeVersion, node.getVersion());
                    if (currentNodes.nodeExists(node)) {
                        logger.debug("received a join request for an existing node [{}]", node);
                    } else {
                        try {
                            nodesBuilder.add(node);
                            nodesChanged = true;
                        } catch (IllegalArgumentException e) {
                            results.failure(joinRequest, e);
                            continue;
                        }
                    }
                    results.success(joinRequest);
                }
                // we do this validation quite late to prevent race conditions between nodes joining and importing dangling indices
                // we have to reject nodes that don't support all indices we have in this cluster
                MembershipAction.ensureIndexCompatibility(minNodeVersion, currentState.getMetaData());
                if (nodesChanged) {
                    newState.nodes(nodesBuilder);
                    return results.build(allocationService.reroute(newState.build(), "node_join"));
                } else {
                    // we must return a new cluster state instance to force publishing. This is important
                    // for the joining node to finalize its join and set us as a master
                    return results.build(newState.build());
                }
            } else {
                logger.trace("processing node joins, but we are not the master. current master: {}", currentNodes.getMasterNode());
                throw new NotMasterException("Node [" + currentNodes.getLocalNode() + "] not master for join request");
            }
        }

        @Override
        public void clusterStatePublished(ClusterChangedEvent event) {
            electMaster.logMinimumMasterNodesWarningIfNecessary(event.previousState(), event.state());
        }
    }

    public void handleLeaveRequest(DiscoveryNode node) {
        assert genericThread();
//        if (lifecycleState() != Lifecycle.State.STARTED) {
//            // not started, ignore a node failure
//            return;
//        }
        if (state.get().getDiscoPhase() == Master) {
            // no need to do this under lock, if we're wrong, the task will fail anyhow
            masterService.submitStateUpdateTask(
                "zen-disco-node-left" + "(" + node + "), reason(" + "left" + ")",
                new NodeRemovalClusterStateTaskExecutor.Task(node, "left"),
                ClusterStateTaskConfig.build(Priority.IMMEDIATE),
                nodeRemovalExecutor,
                nodeRemovalExecutor);
        } else if (node.equals(state.get().getClusterState().nodes().getMasterNode())) {
            synchronized(this) {
                if (state.get().getClusterState().nodes().getMasterNode() == node) {
                    logger.debug("handling leave request of {}", node);
                    moveToPinging();
                }
            }
        }
    }

    @Override
    public void publish(ClusterChangedEvent clusterChangedEvent, AckListener ackListener) {
        ClusterState newState = clusterChangedEvent.state();
        assert newState.getNodes().isLocalNodeElectedMaster() : "Shouldn't publish state when not master " + clusterChangedEvent.source();

        // state got changed locally (maybe because another master published to us)
        if (clusterChangedEvent.previousState() != this.state.get().getClusterState()) {
            throw new FailedToCommitClusterStateException("state was mutated while calculating new CS update");
        }

        // here we are not on generic threadpool, but we could possibly switch to that
        final long clusterStateTerm;
        synchronized(stateMutex) {
            ZenState zenState = state.get();
            if (clusterChangedEvent.previousState() != zenState.getClusterState()) {
                throw new FailedToCommitClusterStateException("state was mutated while calculating new CS update, expected " +
                    clusterChangedEvent.previousState() + " but is " + zenState.getClusterState());
            }
            if (zenState.getDiscoPhase() != Master) {
                throw new FailedToCommitClusterStateException("discostate not master anymore: " + zenState.getDiscoPhase());
            }
            clusterStateTerm = zenState.getNodeTerm();
            // persist state locally (term is not needed, it is already correctly set), then set it in the next line
            // catch exception while persisting and rethrow as FailedToCommitClusterStateException,
            // and also rejoin, which happens by calling moveToPinging()
            state.getAndUpdate(st -> st.withPhaseAndState(DiscoPhase.Master, clusterStateTerm, newState));
            nodesFD.updateNodesAndPing(this.state.get().getClusterState()); // enable ping detection for master
        }

        try {
            publishClusterState.publish(clusterStateTerm, clusterChangedEvent, electMaster.minimumMasterNodes(), ackListener);
        } catch (FailedToCommitClusterStateException t) {
            // cluster service logs a WARN message
            logger.debug("failed to publish cluster state version [{}] (not enough nodes acknowledged, min master nodes [{}])",
                newState.version(), electMaster.minimumMasterNodes());

            synchronized (stateMutex) {
//                publishClusterState.pendingStatesQueue().failAllStatesAndClear(
//                    new ElasticsearchException("failed to publish cluster state"));

                if (state.get().getDiscoPhase() == Master && state.get().getNodeTerm() == clusterStateTerm) {
                    moveToPinging();
                } else {
                    // ignore, someone else took already care of this
                }
            }
            throw t;
        }



        // apply cluster state (should we do this under state mutex? Cluster state versions and applier should deal with whether CS is older / newer than current)
        // what if failure while applying locally?
        // what if state has since changed when applying locally?
        // indefinitely wait for cluster state to be applied locally
        try {
            CountDownLatch latch = new CountDownLatch(1);
            final DiscoveryNode localNode = newState.getNodes().getLocalNode();
            ClusterStateTaskListener listener = new ClusterStateTaskListener() {
                @Override
                public void onFailure(String source, Exception e) {
                    latch.countDown();
                    ackListener.onNodeAck(localNode, e);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    latch.countDown();
                    ackListener.onNodeAck(localNode, null);
                }
            };
            handleApply(newState, listener);
            latch.await();
        } catch (InterruptedException e) {
            logger.debug(
                (Supplier<?>) () -> new ParameterizedMessage(
                    "interrupted while applying cluster state locally [{}]",
                    clusterChangedEvent.source()),
                e);
            Thread.currentThread().interrupt();
        }
    }

    public void handleIncomingClusterState(long term, ClusterState clusterState) {
        assert genericThread();
        long currentNodeTerm = state.get().getNodeTerm();
        if (term < currentNodeTerm) {
            // ok outside synchronized block as currentNodeTerm only goes up
            throw new RuntimeException("term too old (incoming: " + term + " current: " + currentNodeTerm + ")");
        }
        synchronized(stateMutex) {
            ZenState zenState = state.get();
            if (term < zenState.getNodeTerm()) {
                throw new RuntimeException("term too old");
            } else if (term > zenState.getNodeTerm()) {
                moveToFollower(term, clusterState.nodes().getMasterNode());
            } else {
                assert term == zenState.getNodeTerm();
                assert zenState.getDiscoPhase() != Master : "Split brain, two masters for same term";
                if (zenState.getDiscoPhase() != DiscoPhase.Follower) {
                    moveToFollower(term, clusterState.nodes().getMasterNode());
                }
            }
            if (betterCS(term, clusterState, zenState.getClusterStateTerm(), zenState.getClusterState())) {
                // TODO: write state to disk and set state to clusterState
                state.getAndUpdate(st -> st.withPhaseAndState(Follower, term, clusterState));
                // also add to a pendingApplyQueue (so that it's clear this CS is waiting to be applied, and if it took us longer than 30 sec to receive it here, we
                // are still able to find it and apply it when the Apply notification comes in)
                pendingApplyQueue.put(clusterState.stateUUID(), clusterState);
            } else {
                throw new RuntimeException("have better state already, got " + zenState.getClusterState() + " but received " + clusterState);
            }
        }
    }

    public static boolean betterCS(long term1, ClusterState cs1, long term2, ClusterState cs2) {
        return term1 > term2 || (term1 == term2 && cs1.getVersion() > cs2.getVersion());
    }


    public void handleApply(ClusterState stateToApply, ClusterStateTaskListener appliedListener) {
        assert genericThread();
        synchronized(stateMutex) {
            if (state.get().getDiscoPhase() != Master && state.get().getDiscoPhase() != Follower) {
                appliedListener.onFailure("apply-locally", new RuntimeException("neither master nor follower"));
                return;
            }
            if (stateToApply.version() > lastCommittedState.getVersion()) {
                lastCommittedState = stateToApply;
                clusterApplier.onNewClusterState("apply cluster state",
                    () -> this.lastCommittedState,
                    appliedListener);
            } else {
                // ignore or fail? we have already applied a better CS
                // appliedListener.finished(); or appliedListener.failure();
                appliedListener.onFailure("apply-locally", new RuntimeException("state has lower version than what's currently committed"));
            }
        }
    }

    public void handleApply(String clusterStateUuid, ClusterStateTaskListener appliedListener) {
        assert genericThread();
        ClusterState stateToApply;
        ClusterState currentState = state.get().getClusterState();
        if (currentState.stateUUID().equals(clusterStateUuid)) {
            stateToApply = currentState;
        } else {
            stateToApply = pendingApplyQueue.get(clusterStateUuid);
        }
        // wrap this appliedListener so that it removes the entry from the pendingApplyQueue once it's finished applying
        handleApply(stateToApply, new ClusterStateTaskListener() {
            @Override
            public void onFailure(String source, Exception e) {
                pendingApplyQueue.remove(clusterStateUuid);
                appliedListener.onFailure(source, e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                pendingApplyQueue.remove(clusterStateUuid);
                appliedListener.clusterStateProcessed(source, oldState, newState);
            }
        });
    }


    private boolean genericThread() {
        return true;
    }

    @Override
    protected void doStop() {
        masterFD.stop("zen disco stop");
        nodesFD.stop();
        Releasables.close(zenPing); // stop any ongoing pinging
        DiscoveryNodes nodes = clusterState().nodes();
        if (sendLeaveRequest) {
            if (nodes.getMasterNode() == null) {
                // if we don't know who the master is, nothing to do here
            } else if (!nodes.isLocalNodeElectedMaster()) {
                try {
                    membership.sendLeaveRequestBlocking(nodes.getMasterNode(), nodes.getLocalNode(), TimeValue.timeValueSeconds(1));
                } catch (Exception e) {
                    logger.debug((Supplier<?>) () -> new ParameterizedMessage("failed to send leave request to master [{}]", nodes.getMasterNode()), e);
                }
            } else {
                // we're master -> let other potential master we left and start a master election now rather then wait for masterFD
                DiscoveryNode[] possibleMasters = electMaster.nextPossibleMasters(nodes.getNodes().values(), 5);
                for (DiscoveryNode possibleMaster : possibleMasters) {
                    if (nodes.getLocalNode().equals(possibleMaster)) {
                        continue;
                    }
                    try {
                        membership.sendLeaveRequest(nodes.getLocalNode(), possibleMaster);
                    } catch (Exception e) {
                        logger.debug((Supplier<?>) () -> new ParameterizedMessage("failed to send leave request from master [{}] to possible master [{}]", nodes.getMasterNode(), possibleMaster), e);
                    }
                }
            }
        }
    }

    @Override
    protected void doClose() throws IOException {
        IOUtils.close(masterFD, nodesFD);
    }

    public ClusterState clusterState() {
        ZenState zenState = state.get();
        assert zenState != null : "accessing cluster state before it is set";
        return zenState.getClusterState();
    }

    /**
     * Gets the current set of nodes involved in the node fault detection.
     * NB: for testing purposes
     */
    Set<DiscoveryNode> getFaultDetectionNodes() {
        return nodesFD.getNodes();
    }

    @Override
    public DiscoveryStats stats() {
        return new DiscoveryStats(
            new PendingClusterStateStats(pendingApplyQueue.size(), pendingApplyQueue.size(), pendingApplyQueue.size()));
    }

    private void submitRejoin(String source) {
        synchronized (stateMutex) {
            logger.debug("rejoin was submitted, moving back to pinging");
            moveToPinging();
//            rejoin(source);
        }
    }

    // visible for testing
    void setState(ClusterState clusterState) {
        synchronized (stateMutex) {
            state.getAndUpdate(zs -> zs.withPhaseAndState(zs.getDiscoPhase(), zs.getClusterStateTerm(), clusterState));
        }
    }

    @Override
    public ZenState currentState() {
        return state.get();
    }

    // visible for testing
    static class NodeRemovalClusterStateTaskExecutor implements ClusterStateTaskExecutor<NodeRemovalClusterStateTaskExecutor.Task>, ClusterStateTaskListener {

        private final AllocationService allocationService;
        private final ElectMasterService electMasterService;
        private final Consumer<String> rejoin;
        private final Logger logger;

        static class Task {

            private final DiscoveryNode node;
            private final String reason;

            Task(final DiscoveryNode node, final String reason) {
                this.node = node;
                this.reason = reason;
            }

            public DiscoveryNode node() {
                return node;
            }

            public String reason() {
                return reason;
            }

            @Override
            public String toString() {
                return node + " " + reason;
            }
        }

        NodeRemovalClusterStateTaskExecutor(
                final AllocationService allocationService,
                final ElectMasterService electMasterService,
                final Consumer<String> rejoin,
                final Logger logger) {
            this.allocationService = allocationService;
            this.electMasterService = electMasterService;
            this.rejoin = rejoin;
            this.logger = logger;
        }

        @Override
        public ClusterTasksResult<Task> execute(final ClusterState currentState, final List<Task> tasks) throws Exception {
            final DiscoveryNodes.Builder remainingNodesBuilder = DiscoveryNodes.builder(currentState.nodes());
            boolean removed = false;
            for (final Task task : tasks) {
                if (currentState.nodes().nodeExists(task.node())) {
                    remainingNodesBuilder.remove(task.node());
                    removed = true;
                } else {
                    logger.debug("node [{}] does not exist in cluster state, ignoring", task);
                }
            }

            if (!removed) {
                // no nodes to remove, keep the current cluster state
                return ClusterTasksResult.<Task>builder().successes(tasks).build(currentState);
            }

            final ClusterState remainingNodesClusterState = remainingNodesClusterState(currentState, remainingNodesBuilder);

            final ClusterTasksResult.Builder<Task> resultBuilder = ClusterTasksResult.<Task>builder().successes(tasks);
            if (electMasterService.hasEnoughMasterNodes(remainingNodesClusterState.nodes()) == false) {
                final int masterNodes = electMasterService.countMasterNodes(remainingNodesClusterState.nodes());
                rejoin.accept(LoggerMessageFormat.format("not enough master nodes (has [{}], but needed [{}])",
                                                         masterNodes, electMasterService.minimumMasterNodes()));
                return resultBuilder.build(currentState);
            } else {
                return resultBuilder.build(allocationService.deassociateDeadNodes(remainingNodesClusterState, true, describeTasks(tasks)));
            }
        }

        // visible for testing
        // hook is used in testing to ensure that correct cluster state is used to test whether a
        // rejoin or reroute is needed
        ClusterState remainingNodesClusterState(final ClusterState currentState, DiscoveryNodes.Builder remainingNodesBuilder) {
            return ClusterState.builder(currentState).nodes(remainingNodesBuilder).build();
        }

        @Override
        public void onFailure(final String source, final Exception e) {
            logger.error((Supplier<?>) () -> new ParameterizedMessage("unexpected failure during [{}]", source), e);
        }

        @Override
        public void onNoLongerMaster(String source) {
            logger.debug("no longer master while processing node removal [{}]", source);
        }

    }

    private void removeNode(final DiscoveryNode node, final String source, final String reason) {
        masterService.submitStateUpdateTask(
                source + "(" + node + "), reason(" + reason + ")",
                new NodeRemovalClusterStateTaskExecutor.Task(node, reason),
                ClusterStateTaskConfig.build(Priority.IMMEDIATE),
                nodeRemovalExecutor,
                nodeRemovalExecutor);
    }

    private void handleNodeFailure(final DiscoveryNode node, final String reason) {
        if (lifecycleState() != Lifecycle.State.STARTED) {
            // not started, ignore a node failure
            return;
        }
        if (!localNodeMaster()) {
            // nothing to do here...
            return;
        }
        removeNode(node, "zen-disco-node-failed", reason);
    }

    private void handleMinimumMasterNodesChanged(final int minimumMasterNodes) {
        if (lifecycleState() != Lifecycle.State.STARTED) {
            // not started, ignore a node failure
            return;
        }
        final int prevMinimumMasterNode = ZenDiscovery2.this.electMaster.minimumMasterNodes();
        ZenDiscovery2.this.electMaster.minimumMasterNodes(minimumMasterNodes);
        if (!localNodeMaster()) {
            // We only set the new value. If the master doesn't see enough nodes it will revoke it's mastership.
            return;
        }
        synchronized (stateMutex) {
            // check if we have enough master nodes, if not, we need to move into joining the cluster again
            if (!electMaster.hasEnoughMasterNodes(state.get().getClusterState().nodes())) {
                logger.debug("minimum master nodes too low, going back to pinging");
                moveToPinging();
                //rejoin("not enough master nodes on change of minimum_master_nodes from [" + prevMinimumMasterNode + "] to [" + minimumMasterNodes + "]");
            }
        }
    }

    public void handleMasterGone(final DiscoveryNode masterNode, final Throwable cause, final String reason) {
        if (lifecycleState() != Lifecycle.State.STARTED) {
            // not started, ignore a master failure
            return;
        }
        if (localNodeMaster()) {
            // we might get this on both a master telling us shutting down, and then the disconnect failure
            return;
        }

        logger.info((Supplier<?>) () -> new ParameterizedMessage("master_left [{}], reason [{}]", masterNode, reason), cause);

        synchronized (stateMutex) {
            // TODO: also take term into account here, which requires term management from fault detection
            if (state.get().getDiscoPhase() == Follower) {
                logger.debug("master is gone, moving back to pinging");
                moveToPinging();
            }
//            if (localNodeMaster() == false && masterNode.equals(state.get().getClusterState().nodes().getMasterNode())) {
//                moveToPinging();
//                // flush any pending cluster states from old master, so it will not be set as master again
////                publishClusterState.pendingStatesQueue().failAllStatesAndClear(new ElasticsearchException("master left [{}]", reason));
////                rejoin("master left (reason = " + reason + ")");
//            }
        }
    }

    void handleJoinRequest(final JoinRequest joinRequest, final ClusterState state, final MembershipAction.JoinCallback callback) {
        DiscoveryNode node = joinRequest.getDiscoveryNode();
        // we do this in a couple of places including the cluster update thread. This one here is really just best effort
        // to ensure we fail as fast as possible.
        MembershipAction.ensureIndexCompatibility(node.getVersion(), state.getMetaData());
        // try and connect to the node, if it fails, we can raise an exception back to the client...
        transportService.connectToNode(node);

        // validate the join request, will throw a failure if it fails, which will get back to the
        // node calling the join request
        try {
            membership.sendValidateJoinRequestBlocking(node, state, joinTimeout);
        } catch (Exception e) {
            logger.warn((Supplier<?>) () -> new ParameterizedMessage("failed to validate incoming join request from node [{}]", node), e);
            callback.onFailure(new IllegalStateException("failure when sending a validation request to node", e));
            return;
        }
        handleJoinRequest(joinRequest, callback);
    }

    private boolean localNodeMaster() {
        return clusterState().nodes().isLocalNodeElectedMaster();
    }

    private void handleAnotherMaster(ClusterState localClusterState, final DiscoveryNode otherMaster, long otherClusterStateVersion, String reason) {
        assert localClusterState.nodes().isLocalNodeElectedMaster() : "handleAnotherMaster called but current node is not a master";
        assert Thread.holdsLock(stateMutex);

        if (otherClusterStateVersion > localClusterState.version()) {
            //TODO : take terms into account here
            // rejoin("zen-disco-discovered another master with a new cluster_state [" + otherMaster + "][" + reason + "]");
        } else {
            // TODO: do this outside mutex
            logger.warn("discovered [{}] which is also master but with an older cluster_state, telling [{}] to rejoin the cluster ([{}])", otherMaster, otherMaster, reason);
            try {
                // make sure we're connected to this node (connect to node does nothing if we're already connected)
                // since the network connections are asymmetric, it may be that we received a state but have disconnected from the node
                // in the past (after a master failure, for example)
                transportService.connectToNode(otherMaster);
                transportService.sendRequest(otherMaster, ZenDiscovery.DISCOVERY_REJOIN_ACTION_NAME, new RejoinClusterRequest(localClusterState.nodes().getLocalNodeId()), new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {

                    @Override
                    public void handleException(TransportException exp) {
                        logger.warn((Supplier<?>) () -> new ParameterizedMessage("failed to send rejoin request to [{}]", otherMaster), exp);
                    }
                });
            } catch (Exception e) {
                logger.warn((Supplier<?>) () -> new ParameterizedMessage("failed to send rejoin request to [{}]", otherMaster), e);
            }
        }
    }

    /**
     * returns true if zen discovery is started and there is a currently a background thread active for (re)joining
     * the cluster used for testing.
     */
    public boolean joiningCluster() {
        final DiscoPhase discoPhase = state.get().getDiscoPhase();
        return discoPhase == DiscoPhase.Pinging || discoPhase == DiscoPhase.Become_Follower || discoPhase == DiscoPhase.Become_Master;
    }

    public DiscoverySettings getDiscoverySettings() {
        return discoverySettings;
    }

    @Override
    public void onIncomingClusterState(long term, ClusterState incomingState) {
        handleIncomingClusterState(term, incomingState);
    }

    @Override
    public void onClusterStateCommitted(String stateUUID, ActionListener<Void> processedListener) {
        handleApply(stateUUID, new ClusterStateTaskListener() {

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                processedListener.onResponse(null);
            }

            @Override
            public void onFailure(String source, Exception e) {
                processedListener.onFailure(e);
            }
        });
    }

    private class MembershipListener implements MembershipAction.MembershipListener {
        @Override
        public void onJoin(JoinRequest joinRequest, MembershipAction.JoinCallback callback) {
            handleJoinRequest(joinRequest, ZenDiscovery2.this.clusterState(), callback);
        }

        @Override
        public void onLeave(DiscoveryNode node) {
            handleLeaveRequest(node);
        }
    }

    private class NodeFaultDetectionListener extends NodesFaultDetection.Listener {

        private final AtomicInteger pingsWhileMaster = new AtomicInteger(0);

        @Override
        public void onNodeFailure(DiscoveryNode node, String reason) {
            handleNodeFailure(node, reason);
        }

        @Override
        public void onPingReceived(final NodesFaultDetection.PingRequest pingRequest) {
            // if we are master, we don't expect any fault detection from another node. If we get it
            // means we potentially have two masters in the cluster.
            if (!localNodeMaster()) {
                pingsWhileMaster.set(0);
                return;
            }

            if (pingsWhileMaster.incrementAndGet() < maxPingsFromAnotherMaster) {
                logger.trace("got a ping from another master {}. current ping count: [{}]", pingRequest.masterNode(), pingsWhileMaster.get());
                return;
            }
            logger.debug("got a ping from another master {}. resolving who should rejoin. current ping count: [{}]", pingRequest.masterNode(), pingsWhileMaster.get());
            synchronized (stateMutex) {
                ClusterState currentState = state.get().getClusterState();
                if (currentState.nodes().isLocalNodeElectedMaster()) {
                    pingsWhileMaster.set(0);
                    handleAnotherMaster(currentState, pingRequest.masterNode(), pingRequest.clusterStateVersion(), "node fd ping");
                }
            }
        }
    }

    private class MasterNodeFailureListener implements MasterFaultDetection.Listener {

        @Override
        public void onMasterFailure(DiscoveryNode masterNode, Throwable cause, String reason) {
            handleMasterGone(masterNode, cause, reason);
        }
    }

    public static class RejoinClusterRequest extends TransportRequest {

        private String fromNodeId;

        RejoinClusterRequest(String fromNodeId) {
            this.fromNodeId = fromNodeId;
        }

        public RejoinClusterRequest() {
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            fromNodeId = in.readOptionalString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalString(fromNodeId);
        }
    }

    class RejoinClusterRequestHandler implements TransportRequestHandler<RejoinClusterRequest> {
        @Override
        public void messageReceived(final RejoinClusterRequest request, final TransportChannel channel) throws Exception {
            try {
                channel.sendResponse(TransportResponse.Empty.INSTANCE);
            } catch (Exception e) {
                logger.warn("failed to send response on rejoin cluster request handling", e);
            }
            synchronized (stateMutex) {
                //rejoin("received a request to rejoin the cluster from [" + request.fromNodeId + "]");
                logger.debug("handled request to rejoin the cluster, moving back to pinging");
                moveToPinging();
            }
        }
    }
}
