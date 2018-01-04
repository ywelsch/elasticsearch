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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.zen2.ConsensusState.CommittedState;
import org.elasticsearch.discovery.zen2.ConsensusState.NodeCollection;
import org.elasticsearch.discovery.zen2.Messages.ApplyCommit;
import org.elasticsearch.discovery.zen2.Messages.CatchupRequest;
import org.elasticsearch.discovery.zen2.Messages.HeartbeatRequest;
import org.elasticsearch.discovery.zen2.Messages.HeartbeatResponse;
import org.elasticsearch.discovery.zen2.Messages.LegislatorPublishResponse;
import org.elasticsearch.discovery.zen2.Messages.OfferVote;
import org.elasticsearch.discovery.zen2.Messages.PublishRequest;
import org.elasticsearch.discovery.zen2.Messages.PublishResponse;
import org.elasticsearch.discovery.zen2.Messages.SeekVotes;
import org.elasticsearch.discovery.zen2.Messages.StartVoteRequest;
import org.elasticsearch.discovery.zen2.Messages.Vote;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.EmptyTransportResponseHandler;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponseHandler;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * A Legislator, following the Paxos metaphor, is responsible for making sure that actions happen in a timely fashion
 * and for turning messages from the outside world into actions performed on the ConsensusState.
 *
 * @param <T> The state tracked in the replicated state machine.
 */
public class Legislator<T extends CommittedState> extends AbstractComponent {

    public static final Setting<TimeValue> CONSENSUS_MIN_DELAY_SETTING =
        Setting.timeSetting("discovery.zen2.min_delay",
            TimeValue.timeValueMillis(300), TimeValue.timeValueMillis(1), Setting.Property.NodeScope);
    public static final Setting<TimeValue> CONSENSUS_MAX_DELAY_SETTING =
        Setting.timeSetting("discovery.zen2.max_delay",
            TimeValue.timeValueMillis(10000), TimeValue.timeValueMillis(1000), Setting.Property.NodeScope);
    // the time in leader state without being able to contact a quorum of nodes before becoming a candidate again
    public static final Setting<TimeValue> CONSENSUS_LEADER_TIMEOUT_SETTING =
        Setting.timeSetting("discovery.zen2.leader_timeout",
            TimeValue.timeValueMillis(30000), TimeValue.timeValueMillis(1), Setting.Property.NodeScope);
    // the time in follower state without receiving new values or heartbeats from a leader before becoming a candidate again
    public static final Setting<TimeValue> CONSENSUS_FOLLOWER_TIMEOUT_SETTING =
        Setting.timeSetting("discovery.zen2.follower_timeout",
            TimeValue.timeValueMillis(90000), TimeValue.timeValueMillis(1), Setting.Property.NodeScope);
    // the delay at which the leader sends out a heartbeat after becoming leader or after a successful publishing/heartbeat round
    public static final Setting<TimeValue> CONSENSUS_HEARTBEAT_DELAY_SETTING =
        Setting.timeSetting("discovery.zen2.heartbeat_delay",
            settings -> TimeValue.timeValueMillis((CONSENSUS_LEADER_TIMEOUT_SETTING.get(settings).millis() / 3) + 1),
            TimeValue.timeValueMillis(1), Setting.Property.NodeScope);

    private final ConsensusState<T> consensusState;
    private final Transport<T> transport;
    private final DiscoveryNode localNode;
    private final Supplier<List<DiscoveryNode>> nodeSupplier;
    private final Function<T, Diff<T>> noOpCreator;
    private final TimeValue minDelay;
    private final TimeValue maxDelay;
    private final TimeValue leaderTimeout;
    private final TimeValue followerTimeout;
    private final TimeValue heartbeatDelay;
    private final LongSupplier currentTimeSupplier;
    private final Random random;

    private boolean termIncrementedAtLeastOnce; // Ensures we cannot win an election without publishing being permitted

    private Mode mode;
    private LeaderMode leaderMode;
    private Optional<DiscoveryNode> lastKnownLeader;
    private long currentDelayMillis;
    private long nextWakeUpTimeMillis;
    // TODO use nanoseconds throughout instead

    // Present if we are in the pre-voting phase, used to collect vote offers.
    private Optional<OfferVoteCollector> currentOfferVoteCollector;

    // Present if we are collecting heartbeats
    private Optional<HeartbeatCollector> currentHeartbeatCollector;

    // Present if we are catching-up.
    private Optional<PublishRequest<T>> storedPublishRequest = Optional.empty();

    public Legislator(Settings settings, ConsensusState.PersistedState<T> persistedState,
                      Transport<T> transport, DiscoveryNode localNode, LongSupplier currentTimeSupplier,
                      Supplier<List<DiscoveryNode>> nodeSupplier, Function<T, Diff<T>> noOpCreator) {
        super(settings);
        minDelay = CONSENSUS_MIN_DELAY_SETTING.get(settings);
        maxDelay = CONSENSUS_MAX_DELAY_SETTING.get(settings);
        leaderTimeout = CONSENSUS_LEADER_TIMEOUT_SETTING.get(settings);
        followerTimeout = CONSENSUS_FOLLOWER_TIMEOUT_SETTING.get(settings);
        heartbeatDelay = CONSENSUS_HEARTBEAT_DELAY_SETTING.get(settings);
        random = Randomness.get();

        consensusState = new ConsensusState<>(settings, persistedState);
        this.transport = transport;
        this.localNode = localNode;
        this.currentTimeSupplier = currentTimeSupplier;
        this.nodeSupplier = nodeSupplier;
        this.noOpCreator = noOpCreator;
        currentHeartbeatCollector = Optional.empty();
        lastKnownLeader = Optional.empty();
        currentOfferVoteCollector = Optional.empty();

        becomeCandidate("init");
    }

    public Mode getMode() {
        return mode;
    }

    public DiscoveryNode getLocalNode() {
        return localNode;
    }

    public T getCommittedState() {
        return consensusState.getCommittedState();
    }

    /**
     * Returns the (non-negative) remaining delay from now when the next relevant wakeup should happen
     */
    public long getNextWakeUpDelayMillis() {
        return getNextWakeUpDelayMillis(currentTimeSupplier.getAsLong());
    }

    private long getNextWakeUpDelayMillis(long now) {
        return Math.max(nextWakeUpTimeMillis - now, 0L);
    }

    @SuppressForbidden(reason = "Argument to Math.abs() is definitely not Long.MIN_VALUE")
    private long randomNonNegativeLong() {
        long result = random.nextLong();
        return result == Long.MIN_VALUE ? 0 : Math.abs(result);
    }

    /**
     * @return a random long in [lowerBound, upperBound)
     */
    private long randomLongBetween(long lowerBound, long upperBound) {
        assert 0 < upperBound - lowerBound;
        return randomNonNegativeLong() % (upperBound - lowerBound) + lowerBound;
    }

    private void ignoreWakeUpsForRandomDelay() {
        nextWakeUpTimeMillis = currentTimeSupplier.getAsLong() + randomLongBetween(minDelay.getMillis(), currentDelayMillis + 1);
    }

    public void ignoreWakeUpsForAtLeast(TimeValue delay) {
        final long newWakeUpTimeMillis = currentTimeSupplier.getAsLong() + delay.getMillis();
        if (newWakeUpTimeMillis - nextWakeUpTimeMillis > 0L) {
            nextWakeUpTimeMillis = newWakeUpTimeMillis;
        }
    }

    public void handleWakeUp() {
        long now = currentTimeSupplier.getAsLong();
        final long remainingWakeUpDelay = getNextWakeUpDelayMillis(now);

        try {
            if (remainingWakeUpDelay == 0L) {
                final Level logLevel = mode == Mode.LEADER && leaderMode == LeaderMode.HEARTBEAT_DELAY ? Level.TRACE : Level.DEBUG;
                logger.log(logLevel, "handleWakeUp: waking up as [{}] at [{}] with slot={}, term={}, lastAcceptedTerm={}", mode, now,
                    consensusState.firstUncommittedSlot(), consensusState.getCurrentTerm(), consensusState.lastAcceptedTerm());
                switch (mode) {
                    case CANDIDATE:
                        currentDelayMillis = Math.min(maxDelay.getMillis(), currentDelayMillis + minDelay.getMillis());
                        ignoreWakeUpsForRandomDelay();
                        startSeekingVotes();
                        break;

                    case LEADER:
                        switch (leaderMode) {
                            case PUBLISH_IN_PROGRESS:
                            case HEARTBEAT_IN_PROGRESS:
                                becomeCandidate("handleWakeUp");
                                break;
                            case HEARTBEAT_DELAY:
                                becomeOrRenewLeader("handleWakeUp", LeaderMode.HEARTBEAT_IN_PROGRESS);
                                sendHeartBeat();
                                break;
                        }
                        break;

                    case FOLLOWER:
                        becomeCandidate("handleWakeUp");
                        break;
                }
            } else {
                logger.trace("handleWakeUp: ignoring wake-up at [{}], next wake-up delay is [{}]", now,
                    TimeValue.timeValueMillis(getNextWakeUpDelayMillis(now)));
            }
        } finally {
            assert getNextWakeUpDelayMillis(now) > 0L;
        }
    }

    private void becomeCandidate(String method) {
        logger.debug("{}: becoming candidate (lastKnownLeader was [{}])", method, lastKnownLeader);
        mode = Mode.CANDIDATE;
        currentDelayMillis = minDelay.getMillis();
        ignoreWakeUpsForRandomDelay();
        currentHeartbeatCollector = Optional.empty();
    }

    private void becomeOrRenewLeader(String method, LeaderMode newLeaderMode) {
        if (mode != Mode.LEADER) {
            logger.debug("{}: becoming leader [{}] (lastKnownLeader was [{}])", method, newLeaderMode, lastKnownLeader);
        } else {
            assert newLeaderMode != leaderMode;
            // publishing always followed by delaying the heartbeat
            assert leaderMode != LeaderMode.PUBLISH_IN_PROGRESS || newLeaderMode == LeaderMode.HEARTBEAT_DELAY;
            logger.trace("{}: switching leader mode (prev: [{}], now: [{}])", method, leaderMode, newLeaderMode);
        }
        mode = Mode.LEADER;
        leaderMode = newLeaderMode;
        switch (newLeaderMode) {
            case PUBLISH_IN_PROGRESS:
            case HEARTBEAT_IN_PROGRESS:
                ignoreWakeUpsForAtLeast(leaderTimeout);
                break;
            case HEARTBEAT_DELAY:
                ignoreWakeUpsForAtLeast(heartbeatDelay);
                break;
        }
        lastKnownLeader = Optional.of(localNode);
        currentHeartbeatCollector = Optional.empty();
    }

    private void becomeOrRenewFollower(String method, DiscoveryNode leaderNode) {
        if (mode != Mode.FOLLOWER) {
            logger.debug("{}: becoming follower of [{}] (lastKnownLeader was [{}])",
                method, leaderNode, lastKnownLeader);
        }
        mode = Mode.FOLLOWER;
        lastKnownLeader = Optional.of(leaderNode);
        ignoreWakeUpsForAtLeast(followerTimeout);
        currentHeartbeatCollector = Optional.empty();
    }

    private void startSeekingVotes() {
        currentOfferVoteCollector = Optional.of(new OfferVoteCollector());
        SeekVotes seekVotes = new SeekVotes(consensusState.firstUncommittedSlot(), consensusState.getCurrentTerm());
        currentOfferVoteCollector.get().start(seekVotes);
    }

    private void sendHeartBeat() {
        HeartbeatRequest heartbeatRequest =
            new HeartbeatRequest(consensusState.firstUncommittedSlot(), consensusState.getCurrentTerm());
        currentHeartbeatCollector = Optional.of(new HeartbeatCollector());
        safeAddHeartbeatResponse(localNode, currentHeartbeatCollector.get());
        if (currentHeartbeatCollector.isPresent()) {
            currentHeartbeatCollector.get().start(heartbeatRequest);
        }
    }

    public Vote handleStartVote(DiscoveryNode sourceNode, StartVoteRequest startVoteRequest) {
        logger.debug("handleStartVote: from [{}] with term {}", sourceNode, startVoteRequest.getTerm());
        Vote vote = consensusState.handleStartVote(startVoteRequest.getTerm());
        termIncrementedAtLeastOnce = true;
        if (mode != Mode.CANDIDATE) {
            becomeCandidate("handleStartVote");
        }
        return vote;
    }

    private Optional<Vote> ensureTermAtLeast(DiscoveryNode sourceNode, long targetTerm) {
        if (consensusState.getCurrentTerm() < targetTerm) {
            return Optional.of(handleStartVote(sourceNode, new StartVoteRequest(targetTerm)));
        }
        return Optional.empty();
    }

    public void handleClientValue(Diff<T> diff) {
        if (mode != Mode.LEADER) {
            throw new IllegalArgumentException("handleClientValue: not currently leading, so cannot handle client value.");
        }
        PublishRequest<T> publishRequest = consensusState.handleClientValue(diff);
        becomeOrRenewLeader("handleClientValue", LeaderMode.PUBLISH_IN_PROGRESS);
        publish(publishRequest);
    }

    private void publish(PublishRequest<T> publishRequest) {
        CatchupRequest<T> catchUp = new CatchupRequest<>(publishRequest.getTerm(), consensusState.generateCatchup());
        AtomicReference<ApplyCommit> applyCommitReference = new AtomicReference<>();
        nodeSupplier.get().forEach(n -> transport.sendPublishRequest(n, publishRequest,
            new TransportResponseHandler<LegislatorPublishResponse>() {
            @Override
            public LegislatorPublishResponse read(StreamInput in) throws IOException {
                return new LegislatorPublishResponse(in);
            }

            @Override
            public void handleResponse(LegislatorPublishResponse response) {
                if (response.getVote().isPresent()) {
                    handleVote(n, response.getVote().get());
                }
                if (response.isNeedsCatchup()) {
                    logger.debug("handleRequestCatchUp: sending catch-up to {} with slot {}", n, catchUp.getFullState().getSlot());
                    transport.sendCatchUp(n, catchUp, new TransportResponseHandler<PublishResponse>() {
                        @Override
                        public PublishResponse read(StreamInput in) throws IOException {
                            return new PublishResponse(in);
                        }

                        @Override
                        public void handleResponse(PublishResponse response) {
                            handlePublishResponse(n, response);
                        }

                        @Override
                        public void handleException(TransportException exp) {logger.trace(
                            (Supplier<?>) () -> new ParameterizedMessage(
                                "handleCatchupResponse: failed to get catchup response from [{}]", n), exp);
                        }

                        @Override
                        public String executor() {
                            return ThreadPool.Names.SAME;
                        }
                    });
                }
                if (response.getPublishResponse().isPresent()) {
                    handlePublishResponse(n, response.getPublishResponse().get());
                }
            }

            void handlePublishResponse(DiscoveryNode sourceNode, PublishResponse publishResponse) {
                logger.trace("handlePublishResponse: handling [{}] from [{}])", publishResponse, sourceNode);
                assert consensusState.getCurrentTerm() >= publishResponse.getTerm();
                if (applyCommitReference.get() != null) {
                    transport.sendApplyCommit(n, applyCommitReference.get(),
                        new EmptyTransportResponseHandler(ThreadPool.Names.SAME));
                } else {
                    Optional<ApplyCommit> optionalCommit = consensusState.handlePublishResponse(sourceNode, publishResponse);
                    if (optionalCommit.isPresent()) {
                        applyCommitReference.set(optionalCommit.get());
                        nodeSupplier.get().forEach(n -> transport.sendApplyCommit(n, optionalCommit.get(),
                            new EmptyTransportResponseHandler(ThreadPool.Names.SAME)));
                    }
                }
                // TODO: handle negative votes and move to candidate if leader
            }

            @Override
            public void handleException(TransportException exp) {
                logger.trace(
                    (Supplier<?>) () -> new ParameterizedMessage(
                        "handlePublishResponse: failed to get publish response from [{}]", n), exp);
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }
        }));
    }

    void handleVote(DiscoveryNode sourceNode, Vote vote) {
        Optional<Vote> optionalVote = ensureTermAtLeast(localNode, vote.getTerm());
        if (optionalVote.isPresent()) {
            handleVote(localNode, optionalVote.get()); // someone thinks we should be master, so let's try to become one
        }

        if (termIncrementedAtLeastOnce == false) {
            return;
        }

        boolean prevElectionWon = consensusState.electionWon();
        Optional<PublishRequest<T>> maybePublishRequest = consensusState.handleVote(sourceNode, vote);
        assert !prevElectionWon || consensusState.electionWon(); // we cannot go from won to not won
        if (prevElectionWon == false && consensusState.electionWon()) {
            assert mode == Mode.CANDIDATE : "expected candidate but was " + mode;

            becomeOrRenewLeader("handleVote", LeaderMode.PUBLISH_IN_PROGRESS);

            if (maybePublishRequest.isPresent()) {
                publish(maybePublishRequest.get());
            } else {
                assert consensusState.canHandleClientValue();
                publish(consensusState.handleClientValue(noOpCreator.apply(consensusState.getCommittedState())));
            }
        }
    }

    public LegislatorPublishResponse handlePublishRequest(DiscoveryNode sourceNode, PublishRequest<T> publishRequest) {
        if (publishRequest.getTerm() < consensusState.getCurrentTerm()) {
            throw new IllegalArgumentException("incoming term too old");
        }
        if (publishRequest.slot > consensusState.firstUncommittedSlot()) {
            logger.debug("handlePublishRequest: received request [{}] with future slot (expected [{}]): catching up",
                publishRequest, consensusState.firstUncommittedSlot());
            Optional<Vote> optionalVote = ensureTermAtLeast(sourceNode, publishRequest.getTerm());
            if (mode == Mode.LEADER) {
                // we are a stale leader with same term as new leader and have not committed configuration change yet that tells us
                // we're cannot become leader anymore.
                // TODO: Disable electionWon on this node and turn it into follower
                assert false;
            }
            if (sourceNode.equals(localNode) == false) {
                becomeOrRenewFollower("handlePublishRequest", sourceNode);
            }
            storedPublishRequest = Optional.of(publishRequest);
            return new LegislatorPublishResponse(true, Optional.empty(), optionalVote);
        } else {
            logger.trace("handlePublishRequest: handling [{}] from [{}])", publishRequest, sourceNode);
            Optional<Vote> optionalVote = ensureTermAtLeast(sourceNode, publishRequest.getTerm());
            Optional<PublishResponse> optionalPublishResponse = Optional.empty();
            try {
                optionalPublishResponse = Optional.of(consensusState.handlePublishRequest(publishRequest));
                if (sourceNode.equals(localNode) == false) {
                    becomeOrRenewFollower("handlePublishRequest", sourceNode);
                }
            } catch (IllegalArgumentException ignored) {

            }
            return new LegislatorPublishResponse(false, optionalPublishResponse, optionalVote);
        }
    }

    public HeartbeatResponse handleHeartbeatRequest(DiscoveryNode sourceNode, HeartbeatRequest heartbeatRequest) {
        logger.trace("handleHeartbeatRequest: handling [{}] from [{}])", heartbeatRequest, sourceNode);
        assert sourceNode.equals(localNode) == false;
        if (matchesNextSlot(heartbeatRequest)) {
            becomeOrRenewFollower("handleHeartbeatRequest", sourceNode);
        }
        return new HeartbeatResponse(consensusState.firstUncommittedSlot(), consensusState.getCurrentTerm());
    }

    public void handleHeartbeatResponse(DiscoveryNode sourceNode, HeartbeatCollector heartbeatCollector,
                                        HeartbeatResponse heartbeatResponse) {
        if (currentHeartbeatCollector.isPresent() && currentHeartbeatCollector.get() == heartbeatCollector) {
            if (consensusState.firstUncommittedSlot() == heartbeatResponse.getSlot() &&
                consensusState.getCurrentTerm() == heartbeatResponse.getTerm()) {
                safeAddHeartbeatResponse(sourceNode, heartbeatCollector);
            }
        }
    }

    private void safeAddHeartbeatResponse(DiscoveryNode sourceNode, HeartbeatCollector heartbeatCollector) {
        assert mode == Mode.LEADER && leaderMode == LeaderMode.HEARTBEAT_IN_PROGRESS;
        heartbeatCollector.add(sourceNode); // TODO record all the heartbeat responses
        if (consensusState.isQuorumInCurrentConfiguration(heartbeatCollector.heartbeatNodes)) {
            logger.trace("handleHeartbeatResponse: renewing leader lease");
            becomeOrRenewLeader("handleHeartbeatResponse", LeaderMode.HEARTBEAT_DELAY);
        }
    }

    public void handleApplyCommit(DiscoveryNode sourceNode, ApplyCommit applyCommit) {
        logger.trace("handleApplyCommit: applying {}", applyCommit);

        boolean prevElectionWon = consensusState.electionWon();
        consensusState.handleCommit(applyCommit);
        if (prevElectionWon && consensusState.electionWon() && mode == Mode.LEADER) {
            logger.trace("handleApplyCommit: renewing leader lease");
            assert consensusState.canHandleClientValue();
            becomeOrRenewLeader("handleApplyCommit", LeaderMode.HEARTBEAT_DELAY);
        } else if (prevElectionWon == false && consensusState.electionWon()) {
            assert mode != Mode.LEADER : "expected non-leader but was leader";
            assert consensusState.canHandleClientValue();
            becomeOrRenewLeader("handleApplyCommit", LeaderMode.HEARTBEAT_IN_PROGRESS);
            sendHeartBeat();
        } else if (prevElectionWon && consensusState.electionWon() == false) {
            assert mode == Mode.LEADER || mode == Mode.CANDIDATE : localNode.getId() + ": expected leader or candidate but was " + mode;
            if (mode != Mode.CANDIDATE) {
                becomeCandidate("handleApplyCommit");
            }
        }
    }

    public OfferVote handleSeekVotes(DiscoveryNode sender, SeekVotes seekVotes) {
        logger.debug("handleSeekVotes: received [{}] from [{}]", seekVotes, sender);

        if (mode == Mode.CANDIDATE) {
            OfferVote offerVote = new OfferVote(consensusState.firstUncommittedSlot(),
                consensusState.getCurrentTerm(), consensusState.lastAcceptedTerm());
            logger.debug("handleSeekVotes: candidate received [{}] from [{}] and responding with [{}]", seekVotes, sender, offerVote);
            return offerVote;
        } else if (consensusState.getCurrentTerm() < seekVotes.getTerm() && mode == Mode.LEADER) {
            // This is a _rare_ case that can occur if this node is the leader but pauses for long enough for other
            // nodes to consider it failed, leading to `sender` winning a pre-voting round and increments its term, but
            // then this node comes back to life and commits another value in its term, re-establishing itself as the
            // leader and preventing `sender` from winning its election. In this situation all the other nodes end up in
            // mode FOLLOWER and therefore ignore SeekVotes messages from `sender`, but `sender` ignores PublishRequest
            // messages from this node, so is effectively excluded from the cluster for this term. The solution is for
            // this node to perform an election in a yet-higher term so that `sender` can re-join the cluster.
            final long newTerm = seekVotes.getTerm() + 1;
            logger.debug("handleSeekVotes: leader in term {} handling [{}] from [{}] by starting an election in term {}",
                consensusState.getCurrentTerm(), seekVotes, sender, newTerm);
            sendStartVote(new StartVoteRequest(newTerm));
            throw new IllegalArgumentException("I'm still a leader");
        } else {
            // TODO: remove this once it's taken care of by fault detection
            if (mode == Mode.LEADER && consensusState.canHandleClientValue()) {
                becomeOrRenewLeader("handleSeekVotes", LeaderMode.PUBLISH_IN_PROGRESS);
                publish(consensusState.handleClientValue(noOpCreator.apply(consensusState.getCommittedState())));
            }
            logger.debug("handleSeekVotes: not offering vote: slot={}, term={}, mode={}",
                consensusState.firstUncommittedSlot(), consensusState.getCurrentTerm(), mode);
            throw new IllegalArgumentException("not offering vote");
        }
    }

    public class OfferVoteCollector {
        NodeCollection votesOffered = new NodeCollection();
        long maxTermSeen = 0L;

        public void add(DiscoveryNode sender, long term) {
            votesOffered.add(sender);
            maxTermSeen = Math.max(maxTermSeen, term);
        }

        public void start(SeekVotes seekVotes) {
            nodeSupplier.get().forEach(n -> transport.sendSeekVotes(n, seekVotes, new TransportResponseHandler<OfferVote>() {
                @Override
                public OfferVote read(StreamInput in) throws IOException {
                    return new OfferVote(in);
                }

                @Override
                public void handleResponse(OfferVote response) {
                    handleOfferVote(n, OfferVoteCollector.this, response);
                }

                @Override
                public void handleException(TransportException exp) {
                    logger.trace(
                        (Supplier<?>) () -> new ParameterizedMessage(
                            "handleOfferVoteResponse: failed to get vote from [{}]", n), exp);
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }
            }));
        }
    }

    public class HeartbeatCollector {
        NodeCollection heartbeatNodes = new NodeCollection();

        public void add(DiscoveryNode sender) {
            heartbeatNodes.add(sender);
        }

        public void start(HeartbeatRequest heartbeatRequest) {
            nodeSupplier.get().forEach(n -> {
                if (n.equals(localNode) == false) {
                    transport.sendHeartbeatRequest(n, heartbeatRequest,
                        new TransportResponseHandler<HeartbeatResponse>() {
                            @Override
                            public HeartbeatResponse read(StreamInput in) throws IOException {
                                return new HeartbeatResponse(in);
                            }

                            @Override
                            public void handleResponse(HeartbeatResponse response) {
                                handleHeartbeatResponse(n, HeartbeatCollector.this, response);
                            }

                            @Override
                            public void handleException(TransportException exp) {
                                logger.trace(
                                    (Supplier<?>) () -> new ParameterizedMessage(
                                        "handleHeartbeatResponse: failed to get heartbeat from [{}]", n), exp);
                            }

                            @Override
                            public String executor() {
                                return ThreadPool.Names.SAME;
                            }
                        });
                }
            });
        }
    }

    public void handleOfferVote(DiscoveryNode sender, OfferVoteCollector offerVoteCollector, OfferVote offerVote) {
        if (currentOfferVoteCollector.isPresent() == false || currentOfferVoteCollector.get() != offerVoteCollector) {
            logger.debug("handleOfferVote: received OfferVote message from [{}] but not collecting offers.", sender);
            throw new IllegalArgumentException("Received OfferVote but not collecting offers.");
        }

        assert currentOfferVoteCollector.get() == offerVoteCollector;

        if (offerVote.getFirstUncommittedSlot() > consensusState.firstUncommittedSlot()
            || (offerVote.getFirstUncommittedSlot() == consensusState.firstUncommittedSlot()
            && offerVote.getLastAcceptedTerm() > consensusState.lastAcceptedTerm())) {
            logger.debug("handleOfferVote: handing over pre-voting to [{}] because of {}", sender, offerVote);
            currentOfferVoteCollector = Optional.empty();
            transport.sendPreVoteHandover(sender);
            return;
        }

        logger.debug("handleOfferVote: received {} from [{}]", offerVote, sender);
        offerVoteCollector.add(sender, offerVote.getTerm());

        if (consensusState.isQuorumInCurrentConfiguration(offerVoteCollector.votesOffered)) {
            logger.debug("handleOfferVote: received a quorum of OfferVote messages, so starting an election.");
            currentOfferVoteCollector = Optional.empty();
            sendStartVote(new StartVoteRequest(Math.max(consensusState.getCurrentTerm(), offerVoteCollector.maxTermSeen) + 1));
        }
    }

    public void handlePreVoteHandover(DiscoveryNode sender) {
        logger.debug("handlePreVoteHandover: received handover from [{}]", sender);
        startSeekingVotes();
    }

    public boolean matchesNextSlot(Messages.SlotTerm slotTerm) {
        return slotTerm.getTerm() == consensusState.getCurrentTerm() &&
            slotTerm.getSlot() == consensusState.firstUncommittedSlot();
    }

    public PublishResponse handleCatchUp(DiscoveryNode sender, CatchupRequest<T> catchUp) {
        logger.debug("handleCatchUp: received catch-up from {} to slot {}", sender, catchUp.getFullState().getSlot());
        if (catchUp.getTerm() != consensusState.getCurrentTerm()) {
            throw new IllegalArgumentException("catchup for wrong term");
        }
        consensusState.applyCatchup(catchUp.getFullState());
        assert sender.equals(localNode) == false;
        assert mode != Mode.LEADER;

        if (storedPublishRequest.isPresent() && matchesNextSlot(storedPublishRequest.get())) {
            logger.debug("handleCatchUp: replaying stored {}", storedPublishRequest.get());
            // TODO wrong sender on the next line, doesn't matter?
            LegislatorPublishResponse publishResponse = handlePublishRequest(sender, storedPublishRequest.get());
            assert publishResponse.getPublishResponse().isPresent();
            storedPublishRequest = Optional.empty();
            return publishResponse.getPublishResponse().get();
        } else {
            throw new IllegalArgumentException("publish state not stored");
        }
    }

    public void abdicateTo(DiscoveryNode newLeader) {
        if (mode != Mode.LEADER) {
            logger.debug("abdicateTo: mode={} so cannot abdicate to [{}]", mode, newLeader);
            throw new IllegalArgumentException("abdicateTo: not currently leading, so cannot abdicate.");
        }
        logger.debug("abdicateTo: abdicating to [{}]", newLeader);
        transport.sendAbdication(newLeader, consensusState.getCurrentTerm());
    }

    public void handleAbdication(DiscoveryNode sender, long currentTerm) {
        logger.debug("handleAbdication: accepting abdication from [{}] in term {}", sender, currentTerm);
        sendStartVote(new StartVoteRequest(currentTerm + 1));
    }

    private void sendStartVote(StartVoteRequest startVoteRequest) {
        nodeSupplier.get().forEach(n -> transport.sendStartVote(n, startVoteRequest, new TransportResponseHandler<Vote>() {
            @Override
            public Vote read(StreamInput in) throws IOException {
                return new Vote(in);
            }

            @Override
            public void handleResponse(Vote response) {
                handleVote(n, response);
            }

            @Override
            public void handleException(TransportException exp) {
                logger.debug(
                    (Supplier<?>) () -> new ParameterizedMessage(
                        "handleVoteResponse: failed to get vote from [{}]", n), exp);
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }
        }));
    }

    public void handleDisconnectedNode(DiscoveryNode sender) {
        logger.trace("handleDisconnectedNode: lost connection to leader [{}]", sender);
        if (mode == Mode.FOLLOWER && lastKnownLeader.isPresent() && lastKnownLeader.get().equals(sender)) {
            becomeCandidate("handleDisconnectedNode");
        }
    }

    public void invariant() {
        if (mode == Mode.LEADER) {
            assert consensusState.electionWon();
            assert (leaderMode != LeaderMode.PUBLISH_IN_PROGRESS) == consensusState.canHandleClientValue();
            assert lastKnownLeader.isPresent() && lastKnownLeader.get().equals(localNode);
        } else if (mode == Mode.FOLLOWER) {
            assert consensusState.electionWon() == false;
            assert consensusState.canHandleClientValue() == false; // follows from electionWon == false, but explicitly stated here again
            assert lastKnownLeader.isPresent() && (lastKnownLeader.get().equals(localNode) == false);
        }
        assert currentHeartbeatCollector.isPresent() == (mode == Mode.LEADER && leaderMode == LeaderMode.HEARTBEAT_IN_PROGRESS);
    }

    public enum Mode {
        CANDIDATE, LEADER, FOLLOWER
    }

    public enum LeaderMode {
        PUBLISH_IN_PROGRESS, HEARTBEAT_DELAY, HEARTBEAT_IN_PROGRESS
    }

    public interface Transport<T> {
        void sendPublishRequest(DiscoveryNode destination, PublishRequest<T> publishRequest,
                                TransportResponseHandler<LegislatorPublishResponse> responseHandler);

        void sendCatchUp(DiscoveryNode destination, CatchupRequest<T> catchUp, TransportResponseHandler<PublishResponse> responseHandler);

        void sendHeartbeatRequest(DiscoveryNode destination, HeartbeatRequest heartbeatRequest,
                                  TransportResponseHandler<HeartbeatResponse> responseHandler);

        void sendApplyCommit(DiscoveryNode destination, ApplyCommit applyCommit, EmptyTransportResponseHandler responseHandler);

        void sendSeekVotes(DiscoveryNode destination, SeekVotes seekVotes, TransportResponseHandler<OfferVote> responseHandler);

        void sendStartVote(DiscoveryNode destination, StartVoteRequest startVoteRequest, TransportResponseHandler<Vote> responseHandler);

        void sendPreVoteHandover(DiscoveryNode destination);

        void sendAbdication(DiscoveryNode destination, long currentTerm);
    }
}
