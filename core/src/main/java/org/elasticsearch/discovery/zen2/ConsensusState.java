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
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.zen2.Messages.ApplyCommit;
import org.elasticsearch.discovery.zen2.Messages.PublishRequest;
import org.elasticsearch.discovery.zen2.Messages.PublishResponse;
import org.elasticsearch.discovery.zen2.Messages.Vote;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.discovery.zen2.Messages.NO_TERM;

/**
 * The safety core of the consensus algorithm, which ensures that any two instances
 * of this object anywhere in the cluster have the same committedState if they are
 * at the same slot.
 *
 * @param <T> The state on which we achieve consensus.
 */
public class ConsensusState<T extends ConsensusState.CommittedState> extends AbstractComponent {

    // persisted state
    final PersistedState<T> persistedState;

    // transient state
    private boolean electionWon;
    private NodeCollection joinVotes;
    private boolean publishPermitted;
    private NodeCollection publishVotes;

    public ConsensusState(Settings settings, PersistedState<T> persistedState) {
        super(settings);

        // persisted state
        this.persistedState = persistedState;

        // transient state
        this.electionWon = false;
        this.joinVotes = new NodeCollection();
        this.publishPermitted = false;
        this.publishVotes = new NodeCollection();
    }

    public T getCommittedState() {
        return persistedState.getCommittedState();
    }

    public long getCurrentTerm() {
        return persistedState.getCurrentTerm();
    }

    public Optional<AcceptedState<T>> getAcceptedState() {
        return persistedState.getAcceptedState();
    }

    boolean canHandleClientValue() {
        return electionWon && publishPermitted;
    }

    public boolean isQuorumInCurrentConfiguration(NodeCollection votes) {
        final HashSet<String> intersection = new HashSet<>(getCommittedState().getVotingNodes().nodes.keySet());
        intersection.retainAll(votes.nodes.keySet());
        return intersection.size() * 2 > getCommittedState().getVotingNodes().nodes.size();
    }

    public long firstUncommittedSlot() {
        return getCommittedState().getSlot() + 1;
    }

    public long lastAcceptedTerm() {
        return persistedState.lastAcceptedTerm();
    }

    /**
     * May be safely called at any time to move this instance to a new term. It is vitally important for safety that
     * the resulting Vote is sent to no more than one node.
     *
     * @param newTerm The new term
     * @return A Vote that must be sent to at most one other node.
     * @throws IllegalArgumentException if the arguments were incompatible with the current state of this object.
     */
    public Vote handleStartVote(long newTerm) {
        if (newTerm <= getCurrentTerm()) {
            logger.debug("handleStartVote: ignored as term provided [{}] lower or equal than current term [{}]",
                newTerm, getCurrentTerm());
            throw new IllegalArgumentException("incoming term " + newTerm + " lower than current term " + getCurrentTerm());
        }

        logger.debug("handleStartVote: updating term from [{}] to [{}]", getCurrentTerm(), newTerm);

        persistedState.setCurrentTerm(newTerm);
        assert persistedState.getCurrentTerm() == newTerm;
        joinVotes = new NodeCollection();
        electionWon = false;
        publishPermitted = true;
        publishVotes = new NodeCollection();

        return new Vote(firstUncommittedSlot(), getCurrentTerm(), lastAcceptedTerm());
    }

    /**
     * May be called on receipt of a Vote from the given sourceNode.
     *
     * @param sourceNode The sender of the Vote received.
     * @param vote       The Vote received.
     * @return An optional PublishRequest which, if present, can be broadcast to all peers.
     * @throws IllegalArgumentException if the arguments were incompatible with the current state of this object.
     */
    public Optional<PublishRequest<T>> handleVote(DiscoveryNode sourceNode, Vote vote) {
        if (vote.getTerm() != getCurrentTerm()) {
            logger.debug("handleVote: ignored vote due to term mismatch (expected: [{}], actual: [{}])",
                getCurrentTerm(), vote.getTerm());
            throw new IllegalArgumentException("incoming term " + vote.getTerm() + " does not match current term " + getCurrentTerm());
        }
        if (vote.getFirstUncommittedSlot() > firstUncommittedSlot()) {
            logger.debug("handleVote: ignored vote due to slot mismatch (expected: <=[{}], actual: [{}])",
                firstUncommittedSlot(), vote.getFirstUncommittedSlot());
            throw new IllegalArgumentException("incoming slot " + vote.getFirstUncommittedSlot() + " higher than current slot " +
                firstUncommittedSlot());
        }
        final long lastAcceptedTerm = lastAcceptedTerm();
        if (vote.getFirstUncommittedSlot() == firstUncommittedSlot() && vote.getLastAcceptedTerm() > lastAcceptedTerm) {
            logger.debug("handleVote: ignored vote as voter has better last accepted term (expected: <=[{}], actual: [{}])",
                lastAcceptedTerm, vote.getLastAcceptedTerm());
            throw new IllegalArgumentException("incoming last accepted term " + vote.getLastAcceptedTerm() + " higher than " +
                "current last accepted term " + lastAcceptedTerm);
        }

        logger.debug("handleVote: adding vote {} from [{}] for election at slot {}", vote, sourceNode, firstUncommittedSlot());
        joinVotes.add(sourceNode);

        electionWon = isQuorumInCurrentConfiguration(joinVotes);

        logger.debug("handleVote: electionWon={} publishPermitted={} lastAcceptedTerm={}",
            electionWon, publishPermitted, lastAcceptedTerm);
        if (electionWon && publishPermitted && lastAcceptedTerm != NO_TERM) {
            logger.debug("handleVote: sending PublishRequest");

            publishPermitted = false;
            assert getAcceptedState().isPresent(); // must be true because lastAcceptedTerm != NO_TERM
            return Optional.of(new PublishRequest<>(firstUncommittedSlot(), getCurrentTerm(), getAcceptedState().get().getDiff()));
        }

        return Optional.empty();
    }

    /**
     * May be called on receipt of a PublishRequest.
     *
     * @param publishRequest The PublishRequest received.
     * @return A PublishResponse which can be sent back to the sender of the PublishRequest.
     * @throws IllegalArgumentException if the arguments were incompatible with the current state of this object.
     */
    public PublishResponse handlePublishRequest(PublishRequest<T> publishRequest) {
        if (publishRequest.getTerm() != getCurrentTerm()) {
            logger.debug("handlePublishRequest: ignored publish request due to term mismatch (expected: [{}], actual: [{}])",
                getCurrentTerm(), publishRequest.getTerm());
            throw new IllegalArgumentException("incoming term " + publishRequest.getTerm() + " does not match current term " +
                getCurrentTerm());
        }
        if (publishRequest.getSlot() != firstUncommittedSlot()) {
            logger.debug("handlePublishRequest: ignored publish request due to slot mismatch (expected: [{}], actual: [{}])",
                firstUncommittedSlot(), publishRequest.getSlot());
            throw new IllegalArgumentException("incoming slot " + publishRequest.getSlot() + " does not match current slot " +
                firstUncommittedSlot());
        }

        logger.trace("handlePublishRequest: storing publish request for slot [{}] and term [{}]",
            publishRequest.getSlot(), publishRequest.getTerm());
        persistedState.setAcceptedState(publishRequest.getAcceptedState());
        assert persistedState.getAcceptedState().isPresent() &&
            persistedState.getAcceptedState().get() == publishRequest.getAcceptedState();

        return new PublishResponse(publishRequest.getSlot(), publishRequest.getTerm());
    }

    /**
     * May be called on receipt of a PublishResponse from the given sourceNode.
     *
     * @param sourceNode      The sender of the PublishResponse received.
     * @param publishResponse The PublishResponse received.
     * @return An optional ApplyCommit which, if present, may be broadcast to all peers, indicating that this publication
     * has been accepted at a quorum of peers and is therefore committed.
     * @throws IllegalArgumentException if the arguments were incompatible with the current state of this object.
     */
    public Optional<ApplyCommit> handlePublishResponse(DiscoveryNode sourceNode, PublishResponse publishResponse) {
        if (publishResponse.getTerm() != getCurrentTerm()) {
            logger.debug("handlePublishResponse: ignored publish response due to term mismatch (expected: [{}], actual: [{}])",
                getCurrentTerm(), publishResponse.getTerm());
            throw new IllegalArgumentException("incoming term " + publishResponse.getTerm()
                + " does not match current term " + getCurrentTerm());
        }
        if (publishResponse.getSlot() != firstUncommittedSlot()) {
            if (publishResponse.getSlot() == firstUncommittedSlot() - 1) {
                logger.trace("handlePublishResponse: ignored publish response for just-committed slot [{}]", publishResponse.getSlot());
            } else {
                logger.debug("handlePublishResponse: ignored publish response due to slot mismatch (expected: [{}], actual: [{}])",
                    firstUncommittedSlot(), publishResponse.getSlot());
            }
            throw new IllegalArgumentException("incoming slot " + publishResponse.getSlot() + " does not match current slot " +
                firstUncommittedSlot());
        }

        logger.trace("handlePublishResponse: accepted publish response for slot [{}] and term [{}] from [{}]",
            publishResponse.getSlot(), publishResponse.getTerm(), sourceNode);
        publishVotes.add(sourceNode);
        if (isQuorumInCurrentConfiguration(publishVotes)) {
            logger.trace("handlePublishResponse: value committed for slot [{}] and term [{}]",
                firstUncommittedSlot(), getCurrentTerm());
            return Optional.of(new ApplyCommit(publishResponse.getSlot(), publishResponse.getTerm()));
        }

        return Optional.empty();
    }

    /**
     * May be called on receipt of an ApplyCommit. Updates the committed state accordingly.
     *
     * @param applyCommit The ApplyCommit received.
     * @throws IllegalArgumentException if the arguments were incompatible with the current state of this object.
     */
    public void handleCommit(ApplyCommit applyCommit) {
        if (applyCommit.getTerm() != lastAcceptedTerm()) {
            logger.debug("handleCommit: ignored commit request due to term mismatch (expected: [{}], actual: [{}])",
                lastAcceptedTerm(), applyCommit.getTerm());
            throw new IllegalArgumentException("incoming term " + applyCommit.getTerm() + " does not match last accepted term " +
                lastAcceptedTerm());
        }
        if (applyCommit.getSlot() != firstUncommittedSlot()) {
            logger.debug("handleCommit: ignored commit request due to slot mismatch (expected: [{}], actual: [{}])",
                firstUncommittedSlot(), applyCommit.getSlot());
            throw new IllegalArgumentException("incoming slot " + applyCommit.getSlot() + " does not match current slot " +
                firstUncommittedSlot());
        }

        logger.trace("handleCommit: applying commit request for slot [{}]", applyCommit.getSlot());

        assert getAcceptedState().isPresent();
        persistedState.markLastAcceptedStateAsCommitted();
        assert getCommittedState().getSlot() == applyCommit.getSlot();
        assert getAcceptedState().isPresent() == false;
        publishPermitted = true;
        publishVotes = new NodeCollection();
    }

    /**
     * May be safely called at any time. Yields the current committed state to be sent to another, lagging, peer so it can catch up.
     *
     * @return The current committed state.
     */
    public T generateCatchup() {
        logger.debug("generateCatchup: generating catch up for slot [{}]", getCommittedState().getSlot());
        return getCommittedState();
    }

    /**
     * May be called on receipt of a catch-up message containing the current committed state from a peer.
     *
     * @throws IllegalArgumentException if the arguments were incompatible with the current state of this object.
     */
    public void applyCatchup(T newCommittedState) {
        if (newCommittedState.getSlot() <= getCommittedState().getSlot()) {
            logger.debug("applyCatchup: ignored catch up request due to slot mismatch (expected: >[{}], actual: [{}])",
                getCommittedState().getSlot(), newCommittedState.getSlot());
            throw new IllegalArgumentException("incoming slot " + newCommittedState.getSlot() + " higher than current slot " +
                getCommittedState().getSlot());
        }

        logger.debug("applyCatchup: applying catch up for slot [{}]", newCommittedState.getSlot());
        persistedState.setCommittedState(newCommittedState);
        assert persistedState.getCommittedState() == newCommittedState;
        assert persistedState.getAcceptedState().isPresent() == false;
        joinVotes = new NodeCollection();
        electionWon = false;
        publishVotes = new NodeCollection();
        publishPermitted = false;
    }

    /**
     * May be safely called at any time in order to apply the given diff to the replicated state machine.
     *
     * @param diff The RSM transition on which to achieve consensus.
     * @return A PublishRequest that may be broadcast to all peers.
     * @throws IllegalArgumentException if the arguments were incompatible with the current state of this object.
     */
    public PublishRequest<T> handleClientValue(Diff<T> diff) {
        if (electionWon == false) {
            logger.debug("handleClientValue: ignored request as election not won");
            throw new IllegalArgumentException("election not won");
        }
        if (publishPermitted == false) {
            logger.debug("handleClientValue: ignored request as publishing is not permitted");
            throw new IllegalArgumentException("publishing not permitted");
        }
        assert lastAcceptedTerm() == NO_TERM; // see https://github.com/elastic/elasticsearch-formal-models/issues/24

        logger.trace("handleClientValue: processing request for slot [{}] and term [{}]", firstUncommittedSlot(), getCurrentTerm());
        publishPermitted = false;
        return new PublishRequest<>(firstUncommittedSlot(), getCurrentTerm(), diff);
    }

    /**
     * An immutable object representing the current committed state.
     */
    public interface CommittedState extends Writeable {
        long getSlot();

        NodeCollection getVotingNodes();
    }

    public static class AcceptedState<T> implements Writeable {
        protected final long term;
        protected final Diff<T> diff;

        public AcceptedState(long term, Diff<T> diff) {
            this.term = term;
            this.diff = diff;
        }

        public AcceptedState(StreamInput in, Reader<Diff<T>> reader) throws IOException {
            this.term = in.readLong();
            this.diff = reader.read(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(term);
            diff.writeTo(out);
        }

        public long getTerm() {
            return term;
        }

        public Diff<T> getDiff() {
            return diff;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            AcceptedState<?> termDiff = (AcceptedState<?>) o;

            if (term != termDiff.term) return false;
            return diff.equals(termDiff.diff);
        }

        @Override
        public int hashCode() {
            int result = (int) (term ^ (term >>> 32));
            result = 31 * result + diff.hashCode();
            return result;
        }


        @Override
        public String toString() {
            return "AcceptedState{" +
                "term=" + term +
                ", diff=" + diff +
                '}';
        }
    }

    public interface PersistedState<T extends ConsensusState.CommittedState> {

        void setCurrentTerm(long currentTerm);

        void setCommittedState(T committedState);

        void setAcceptedState(AcceptedState<T> acceptedState);

        void markLastAcceptedStateAsCommitted();

        long getCurrentTerm();

        T getCommittedState();

        Optional<AcceptedState<T>> getAcceptedState();

        default long firstUncommittedSlot() {
            return getCommittedState().getSlot() + 1;
        }

        default long lastAcceptedTerm() {
            if (getAcceptedState().isPresent()) {
                return getAcceptedState().get().getTerm();
            } else {
                return NO_TERM;
            }
        }
    }

    public static class BasePersistedState<T extends ConsensusState.CommittedState> implements PersistedState<T> {

        private long currentTerm;
        private T committedState;
        private Optional<AcceptedState<T>> acceptedState;

        public BasePersistedState(long term, T committedState) {
            this.currentTerm = term;
            this.committedState = committedState;
            this.acceptedState = Optional.empty();

            assert currentTerm >= 0;
            assert lastAcceptedTerm() <= currentTerm;
        }

        // copy constructor
        public BasePersistedState(PersistedState<T> persistedState) {
            this.currentTerm = persistedState.getCurrentTerm();
            this.committedState = persistedState.getCommittedState();
            this.acceptedState = persistedState.getAcceptedState();
        }

        @Override
        public void setCurrentTerm(long currentTerm) {
            assert this.currentTerm <= currentTerm;
            this.currentTerm = currentTerm;
        }

        @Override
        public void setCommittedState(T committedState) {
            assert committedState.getSlot() > this.committedState.getSlot();
            this.committedState = committedState;
            acceptedState = Optional.empty();
        }

        @Override
        public void setAcceptedState(AcceptedState<T> acceptedState) {
            this.acceptedState = Optional.of(acceptedState);
        }

        @Override
        public void markLastAcceptedStateAsCommitted() {
            assert acceptedState.isPresent();
            final T newCommittedState = acceptedState.get().getDiff().apply(committedState);
            assert newCommittedState.getSlot() == committedState.getSlot() + 1;
            committedState = newCommittedState;
            acceptedState = Optional.empty();
        }

        @Override
        public long getCurrentTerm() {
            return currentTerm;
        }

        @Override
        public T getCommittedState() {
            return committedState;
        }

        @Override
        public Optional<AcceptedState<T>> getAcceptedState() {
            return acceptedState;
        }
    }

    /**
     * A collection of nodes, used to calculate quorums.
     */
    public static class NodeCollection {

        protected final Map<String, DiscoveryNode> nodes = new HashMap<>();

        public void add(DiscoveryNode sourceNode) {
            // TODO is getId() unique enough or is it user-provided? If the latter, there's a risk of duplicates or of losing votes.
            nodes.put(sourceNode.getId(), sourceNode);
        }

        @Override
        public String toString() {
            return "NodeCollection{" + String.join(",", nodes.keySet()) + "}";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            NodeCollection that = (NodeCollection) o;

            return nodes.equals(that.nodes);
        }

        @Override
        public int hashCode() {
            return nodes.hashCode();
        }
    }
}
