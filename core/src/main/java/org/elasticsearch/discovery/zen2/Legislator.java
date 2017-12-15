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

import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.zen2.ConsensusState.AcceptedState;
import org.elasticsearch.discovery.zen2.ConsensusState.CommittedState;
import org.elasticsearch.discovery.zen2.ConsensusState.NodeCollection;
import org.elasticsearch.discovery.zen2.ConsensusState.Persistence;
import org.elasticsearch.discovery.zen2.Messages.ApplyCommit;
import org.elasticsearch.discovery.zen2.Messages.PublishRequest;
import org.elasticsearch.discovery.zen2.Messages.PublishResponse;
import org.elasticsearch.discovery.zen2.Messages.SeekVotes;
import org.elasticsearch.discovery.zen2.Messages.Vote;

import java.util.Optional;
import java.util.Random;
import java.util.function.LongSupplier;

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
    public static final Setting<TimeValue> CONSENSUS_REELECTION_DELAY_SETTING =
        Setting.timeSetting("discovery.zen2.reelection_delay",
            TimeValue.timeValueMillis(10000), TimeValue.timeValueMillis(1), Setting.Property.NodeScope);
    public static final Setting<TimeValue> CONSENSUS_COMMIT_DELAY_SETTING =
        Setting.timeSetting("discovery.zen2.commit_delay",
            TimeValue.timeValueMillis(90000), TimeValue.timeValueMillis(1), Setting.Property.NodeScope);

    private final Diff<T> noOp;

    private final ConsensusState<T> consensusState;
    private final Transport<T> transport;
    private final DiscoveryNode localNode;
    private final TimeValue minDelay;
    private final TimeValue maxDelay;
    private final TimeValue reelectionDelay;
    private final TimeValue commitDelay;
    private final LongSupplier currentTimeSupplier;
    private final Random random;

    private Mode mode;
    private DiscoveryNode lastKnownLeader;
    private long currentDelayMillis;
    private long nextWakeUpTimeMillis;
    // TODO use nanoseconds throughout instead

    // Present if we are in the pre-voting phase, used to collect vote offers.
    private Optional<NodeCollection> votesOffered = Optional.empty();

    // Present if we are catching-up.
    private Optional<PublishRequest<T>> storedPublishRequest = Optional.empty();

    // Present if we receive an ApplyCommit while catching up
    private Optional<ApplyCommit> storedApplyCommit = Optional.empty();

    public Legislator(Settings settings, long currentTerm, T committedState,
                      Optional<AcceptedState<T>> acceptedState, Persistence<T> persistence,
                      Transport<T> transport, DiscoveryNode localNode, LongSupplier currentTimeSupplier,
                      Diff<T> noOp) {
        super(settings);
        minDelay = CONSENSUS_MIN_DELAY_SETTING.get(settings);
        maxDelay = CONSENSUS_MAX_DELAY_SETTING.get(settings);
        reelectionDelay = CONSENSUS_REELECTION_DELAY_SETTING.get(settings);
        commitDelay = CONSENSUS_COMMIT_DELAY_SETTING.get(settings);
        random = Randomness.get();

        consensusState = new ConsensusState<>(settings, currentTerm, committedState, acceptedState, persistence);
        this.transport = transport;
        this.localNode = localNode;
        this.currentTimeSupplier = currentTimeSupplier;
        this.noOp = noOp;
        lastKnownLeader = localNode;

        mode = Mode.CANDIDATE;

        currentDelayMillis = minDelay.getMillis();
        ignoreWakeUpsForRandomDelay();
    }

    public Mode getMode() {
        return mode;
    }

    public boolean isLeading() {
        return mode == Mode.LEADER || mode == Mode.INCUMBENT;
    }

    public DiscoveryNode getLocalNode() {
        return localNode;
    }

    public T getCommittedState() {
        return consensusState.getCommittedState();
    }

    public long getNextWakeUpTimeMillis() {
        return nextWakeUpTimeMillis;
    }

    @SuppressForbidden(reason = "Argument to Math.abs() is definitely not Long.MIN_VALUE")
    private long randomNonNegativeLong() {
        long result;
        do {
            result = random.nextLong();
        } while (result == Long.MIN_VALUE);
        return Math.abs(result);
    }

    /**
     * @return a random long in [lowerBound, upperBound)
     */
    private long randomLongBetween(long lowerBound, long upperBound) {
        assert 0 < upperBound - lowerBound;
        return randomNonNegativeLong() % (upperBound - lowerBound) + lowerBound;
    }

    private void ignoreWakeUpsForRandomDelay() {
        assert currentDelayMillis > 0;
        assert currentDelayMillis >= minDelay.getMillis();
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

        if (nextWakeUpTimeMillis - now <= 0L) {
            switch (mode) {
                case CANDIDATE:
                    logger.debug("handleWakeUp: waking up as [{}] at [{}] with slot={}, term={}, lastAcceptedTerm={}", mode, now,
                        consensusState.firstUncommittedSlot(), consensusState.getCurrentTerm(), consensusState.lastAcceptedTerm());
                    currentDelayMillis = Math.min(maxDelay.getMillis(), currentDelayMillis + minDelay.getMillis());
                    ignoreWakeUpsForRandomDelay();
                    startSeekingVotes();
                    break;

                case FOLLOWER:
                case INCUMBENT:
                    logger.debug("handleWakeUp: waking up as [{}] at [{}]", mode, now);
                    mode = Mode.CANDIDATE;
                    currentDelayMillis = minDelay.getMillis();
                    ignoreWakeUpsForRandomDelay();
                    break;

                case LEADER:
                    logger.trace("handleWakeUp: waking up as [{}] at [{}]", mode, now);
                    mode = Mode.INCUMBENT;
                    handleClientValue(noOp);
                    ignoreWakeUpsForAtLeast(reelectionDelay);
                    break;
            }
        } else {
            logger.trace("handleWakeUp: ignoring wake-up at [{}] because it is earlier than [{}]", now, nextWakeUpTimeMillis);
        }

        assert nextWakeUpTimeMillis > now;
    }

    private void startSeekingVotes() {
        votesOffered = Optional.of(new NodeCollection());
        SeekVotes seekVotes = new SeekVotes(consensusState.firstUncommittedSlot(), consensusState.getCurrentTerm());
        transport.broadcastSeekVotes(seekVotes);
    }

    public void handleStartVote(DiscoveryNode sourceNode, long targetTerm) {
        logger.debug("handleStartVote: from [{}] with term {}", sourceNode, targetTerm);
        Vote vote = consensusState.handleStartVote(targetTerm);
        transport.sendVote(sourceNode, vote);
    }

    private void ensureTermAtLeast(DiscoveryNode sourceNode, long targetTerm) {
        if (consensusState.getCurrentTerm() < targetTerm) {
            handleStartVote(sourceNode, targetTerm);
        }
    }

    public void handleClientValue(Diff<T> diff) {
        PublishRequest<T> publishRequest = consensusState.handleClientValue(diff);
        transport.broadcastPublishRequest(publishRequest);
    }

    public void handleVote(DiscoveryNode sourceNode, Vote vote) {
        ensureTermAtLeast(localNode, vote.getTerm());

        Optional<PublishRequest<T>> maybePublishRequest = consensusState.handleVote(sourceNode, vote);
        if (maybePublishRequest.isPresent()) {
            PublishRequest<T> publishRequest = maybePublishRequest.get();
            transport.broadcastPublishRequest(publishRequest);
        } else if (consensusState.canHandleClientValue()) {
            handleClientValue(noOp);
        }
    }

    public void handlePublishRequest(DiscoveryNode sourceNode, PublishRequest<T> publishRequest) {
        if (publishRequest.slot > consensusState.firstUncommittedSlot()) {
            logger.debug("handlePublishRequest: received request [{}] with future slot (expected [{}]): catching up",
                publishRequest, consensusState.firstUncommittedSlot());
            storedPublishRequest = Optional.of(publishRequest);
            transport.sendRequestCatchUp(sourceNode);
        } else {
            logger.trace("handlePublishRequest: handling [{}] from [{}])", publishRequest, sourceNode);
            ensureTermAtLeast(sourceNode, publishRequest.getTerm());
            PublishResponse publishResponse = consensusState.handlePublishRequest(publishRequest);
            transport.sendPublishResponse(sourceNode, publishResponse);
        }
    }

    public void handlePublishResponse(DiscoveryNode sourceNode, PublishResponse publishResponse) {
        ensureTermAtLeast(localNode, publishResponse.getTerm());
        consensusState.handlePublishResponse(sourceNode, publishResponse).ifPresent(transport::broadcastApplyCommit);
    }

    public void handleApplyCommit(DiscoveryNode sourceNode, ApplyCommit applyCommit) {

        if (applyCommit.slot > consensusState.firstUncommittedSlot()) {
            logger.debug("handleApplyCommit: storing future {}", applyCommit);
            storedApplyCommit = Optional.of(applyCommit);
            return;
        }

        logger.trace("handleApplyCommit: applying {}", applyCommit);

        consensusState.handleCommit(applyCommit);

        if (sourceNode.equals(localNode)) {
            ignoreWakeUpsForAtLeast(reelectionDelay);
            if (false == isLeading()) {
                logger.debug("handleApplyCommit: becoming leader (lastKnownLeader was [{}])", lastKnownLeader);
            }
            mode = Mode.LEADER;
        } else {
            ignoreWakeUpsForAtLeast(commitDelay);
            if (mode != Mode.FOLLOWER) {
                logger.debug("handleApplyCommit: becoming follower of [{}] (lastKnownLeader was [{}])",
                    sourceNode, lastKnownLeader);
            }
            mode = Mode.FOLLOWER;
        }

        lastKnownLeader = sourceNode;

        storedPublishRequest = Optional.empty();
        storedApplyCommit = Optional.empty();
    }

    public void handleSeekVotes(DiscoveryNode sender, Messages.SeekVotes seekVotes) {
        logger.debug("handleSeekVotes: received [{}] from [{}]", seekVotes, sender);

        if (mode == Mode.CANDIDATE) {
            Messages.OfferVote offerVote = new Messages.OfferVote(consensusState.firstUncommittedSlot(),
                consensusState.getCurrentTerm(), consensusState.lastAcceptedTerm());
            logger.debug("handleSeekVotes: candidate received [{}] from [{}] and responding with [{}]", seekVotes, sender, offerVote);
            transport.sendOfferVote(sender, offerVote);
        } else if (consensusState.getCurrentTerm() < seekVotes.getTerm() && isLeading()) {
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
            transport.broadcastStartVote(newTerm);
        } else {
            logger.debug("handleSeekVotes: not offering vote: slot={}, term={}, mode={}",
                consensusState.firstUncommittedSlot(), consensusState.getCurrentTerm(), mode);
        }
    }

    public void handleOfferVote(DiscoveryNode sender, Messages.OfferVote offerVote) {
        if (votesOffered.isPresent() == false) {
            logger.debug("handleOfferVote: received OfferVote message from [{}] but not collecting offers.", sender);
            throw new IllegalArgumentException("Received OfferVote but not collecting offers.");
        }

        if (offerVote.getFirstUncommittedSlot() > consensusState.firstUncommittedSlot()
                || (offerVote.getFirstUncommittedSlot() == consensusState.firstUncommittedSlot()
                        && offerVote.getLastAcceptedTerm() > consensusState.lastAcceptedTerm())) {
            logger.debug("handleOfferVote: handing over pre-voting to [{}] because of {}", sender, offerVote);
            votesOffered = Optional.empty();
            transport.sendPreVoteHandover(sender);
            return;
        }

        logger.debug("handleOfferVote: received {} from [{}]", offerVote, sender);
        ensureTermAtLeast(localNode, offerVote.getTerm());
        votesOffered.get().add(sender);

        if (consensusState.isQuorumInCurrentConfiguration(votesOffered.get())) {
            logger.debug("handleOfferVote: received a quorum of OfferVote messages, so starting an election.");
            votesOffered = Optional.empty();
            transport.broadcastStartVote(consensusState.getCurrentTerm() + 1);
        }
    }

    public void handlePreVoteHandover(DiscoveryNode sender) {
        logger.debug("handlePreVoteHandover: received handover from [{}]", sender);
        startSeekingVotes();
    }

    public void handleRequestCatchUp(DiscoveryNode sender) {
        assert sender.equals(localNode) == false;
        T committedState = consensusState.generateCatchup();
        logger.debug("handleRequestCatchUp: sending catch-up to {} with slot {}", sender, committedState.getSlot());
        transport.sendCatchUp(sender, committedState);
    }

    public void handleCatchUp(DiscoveryNode sender, T catchUp) {
        logger.debug("handleCatchUp: received catch-up from {} to slot {}", sender, catchUp.getSlot());
        consensusState.applyCatchup(catchUp);
        if (storedPublishRequest.isPresent()) {
            logger.debug("handleCatchUp: replaying stored {}", storedPublishRequest.get());
            handlePublishRequest(sender, storedPublishRequest.get()); // TODO wrong sender, doesn't matter?
            storedPublishRequest = Optional.empty();
        }
        if (storedApplyCommit.isPresent()) {
            logger.debug("handleCatchUp: replaying stored {}", storedApplyCommit.get());
            handleApplyCommit(sender, storedApplyCommit.get()); // TODO wrong sender, doesn't matter?
            storedApplyCommit = Optional.empty();
        }
    }

    public void abdicateTo(DiscoveryNode newLeader) {
        if (isLeading() == false) {
            logger.debug("abdicateTo: mode={} so cannot abdicate to [{}]", mode, newLeader);
            throw new IllegalArgumentException("abdicateTo: not currently leading, so cannot abdicate.");
        }

        logger.debug("abdicateTo: abdicating to [{}]", newLeader);
        transport.sendAbdication(newLeader, consensusState.getCurrentTerm());
    }

    public void handleAbdication(DiscoveryNode sender, long currentTerm) {
        logger.debug("handleAbdication: accepting abdication from [{}] in term {}", sender, currentTerm);
        transport.broadcastStartVote(currentTerm + 1);
    }

    public void handleDisconnectedNode(DiscoveryNode sender) {
        if (mode == Mode.FOLLOWER && sender.equals(lastKnownLeader)) {
            logger.debug("handleDisconnectedNode: lost connection to leader [{}]", sender);
            nextWakeUpTimeMillis = currentTimeSupplier.getAsLong();
            handleWakeUp();
        }
    }

    public enum Mode {
        CANDIDATE, FOLLOWER, INCUMBENT, LEADER
    }

    public interface Transport<T> {
        void sendVote(DiscoveryNode destination, Vote vote);

        void broadcastPublishRequest(PublishRequest<T> publishRequest);

        void sendPublishResponse(DiscoveryNode destination, PublishResponse publishResponse);

        void broadcastApplyCommit(ApplyCommit applyCommit);

        void broadcastSeekVotes(Messages.SeekVotes seekVotes);

        void sendOfferVote(DiscoveryNode destination, Messages.OfferVote offerVote);

        void broadcastStartVote(long term);

        void sendPreVoteHandover(DiscoveryNode destination);

        void sendRequestCatchUp(DiscoveryNode destination);

        void sendCatchUp(DiscoveryNode destination, T catchUp);

        void sendAbdication(DiscoveryNode destination, long currentTerm);
    }
}
