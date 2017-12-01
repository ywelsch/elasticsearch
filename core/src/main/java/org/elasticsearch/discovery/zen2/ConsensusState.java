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
import org.elasticsearch.common.settings.Settings;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;

public class ConsensusState<T extends ConsensusState.CommittedState> extends AbstractComponent {
    public static final long NO_TERM = -1L;

    // persisted state
    long currentTerm;
    T committedState;
    Optional<SlotTermDiff<T>> acceptedState;

    // transient state
    ElectionState electionState;
    PublishState publishState;

    final Persistence persistence;

    public ConsensusState(Settings settings, long currentTerm, T committedState, Optional<SlotTermDiff<T>> acceptedState,
                          Persistence<T> persistence) {
        super(settings);

        assert currentTerm >= 0;
        assert acceptedState.isPresent() == false || acceptedState.get().getTerm() <= currentTerm;
        assert acceptedState.isPresent() == false || acceptedState.get().getSlot() <= firstUncommittedSlot();

        this.currentTerm = currentTerm;
        this.committedState = committedState;
        this.acceptedState = acceptedState;
        this.electionState = new ElectionState();
        this.publishState = new PublishState(false);
        this.persistence = persistence;
    }

    public Vote handleStartVote(long newTerm) {
        if (newTerm <= currentTerm) {
            logger.trace("handleStartVote: ignored as term provided [{}] lower or equal than current term [{}]",
                newTerm, currentTerm);
            throw new IllegalArgumentException("incoming term " + newTerm + " lower than current term " + currentTerm);
        }

        logger.trace("handleStartVote: updating term from [{}] to [{}]", currentTerm, newTerm);

        persistence.persistCurrentTerm(newTerm);
        currentTerm = newTerm;
        electionState = new ElectionState();
        publishState = new PublishState(true);

        return new Vote(firstUncommittedSlot(), currentTerm, lastAcceptedTermInSlot());
    }

    public long firstUncommittedSlot() {
        return committedState.getSlot() + 1;
    }

    public long lastAcceptedTermInSlot() {
        if (acceptedState.isPresent() && firstUncommittedSlot() == acceptedState.get().getSlot()) {
            return acceptedState.get().getTerm();
        } else {
            return NO_TERM;
        }
    }

    public Optional<PublishRequest<T>> handleVote(DiscoveryNode sourceNode, Vote vote) {
        if (vote.getTerm() != currentTerm) {
            logger.trace("handleVote: ignored vote due to term mismatch (expected: [{}], actual: [{}])",
                currentTerm, vote.getTerm());
            throw new IllegalArgumentException("incoming term " + vote.getTerm() + " does not match current term " + currentTerm);
        }
        if (vote.getFirstUncommittedSlot() > firstUncommittedSlot()) {
            logger.trace("handleVote: ignored vote due to slot mismatch (expected: <=[{}], actual: [{}])",
                firstUncommittedSlot(), vote.getFirstUncommittedSlot());
            throw new IllegalArgumentException("incoming slot " + vote.getFirstUncommittedSlot() + " higher than current slot " +
                firstUncommittedSlot());
        }
        if (vote.getFirstUncommittedSlot() == firstUncommittedSlot() && vote.getLastAcceptedTerm() != NO_TERM) {
            final long lastAcceptedTermInSlot = lastAcceptedTermInSlot();
            if (vote.getLastAcceptedTerm() > lastAcceptedTermInSlot) {
                logger.trace("handleVote: ignored vote as voter has better last accepted term (expected: <=[{}], actual: [{}])",
                    lastAcceptedTermInSlot, vote.getLastAcceptedTerm());
                throw new IllegalArgumentException("incoming last accepted term " + vote.getLastAcceptedTerm() + " higher than " +
                    "current last accepted term " + lastAcceptedTermInSlot);
            }
            if (vote.getLastAcceptedTerm() < lastAcceptedTermInSlot && electionState.valueForced() == false) {
                logger.trace("handleVote: ignored vote as voter has worse last accepted term and election value not forced " +
                        "(expected: <=[{}], actual: [{}])", lastAcceptedTermInSlot, vote.getLastAcceptedTerm());
                throw new IllegalArgumentException("incoming last accepted term " + vote.getLastAcceptedTerm() + " lower than " +
                    "current last accepted term " + lastAcceptedTermInSlot + " and election value not forced");
            }
            electionState.setValueForced(true);
        }

        logger.trace("handleVote: adding vote {} from {} for election at slot {}", vote, sourceNode, firstUncommittedSlot());
        electionState.add(sourceNode);

        if (electionState.maybeSetElectionWon(committedState.getVotingNodes())) {
            logger.trace("handleVote: election won");

            if (electionState.valueForced()) {
                logger.trace("handleVote: value forced");

                publishState.disablePublishing();
                assert acceptedState.isPresent(); // must be true because electionState.valueForced();
                return Optional.of(new PublishRequest<>(firstUncommittedSlot(), currentTerm, acceptedState.get().getDiff()));
            }
        }

        return Optional.empty();
    }

    public PublishResponse handlePublishRequest(PublishRequest<T> publishRequest) {
        if (publishRequest.getTerm() != currentTerm) {
            logger.trace("handlePublishRequest: ignored publish request due to term mismatch (expected: [{}], actual: [{}])",
                currentTerm, publishRequest.getTerm());
            throw new IllegalArgumentException("incoming term " + publishRequest.getTerm() + " does not match current term " +
                currentTerm);
        }
        if (publishRequest.getSlot() != firstUncommittedSlot()) {
            logger.trace("handlePublishRequest: ignored publish request due to slot mismatch (expected: [{}], actual: [{}])",
                firstUncommittedSlot(), publishRequest.getSlot());
            throw new IllegalArgumentException("incoming slot " + publishRequest.getSlot() + " does not match current slot " +
                firstUncommittedSlot());
        }

        logger.trace("handlePublishRequest: storing publish request for slot [{}] and term [{}]",
            publishRequest.getSlot(), publishRequest.getTerm());
        persistence.persistAcceptedState(publishRequest);
        acceptedState = Optional.of(publishRequest);

        return new PublishResponse(publishRequest.getSlot(), publishRequest.getTerm());
    }

    public Optional<ApplyCommit> handlePublishResponse(DiscoveryNode sourceNode, PublishResponse publishResponse) {
        if (publishResponse.getTerm() != currentTerm) {
            logger.trace("handlePublishResponse: ignored publish response due to term mismatch (expected: [{}], actual: [{}])",
                currentTerm, publishResponse.getTerm());
            throw new IllegalArgumentException("incoming term " + publishResponse.getTerm()
                + " does not match current term " + currentTerm);
        }
        if (publishResponse.getSlot() != firstUncommittedSlot()) {
            logger.trace("handlePublishResponse: ignored publish response due to slot mismatch (expected: [{}], actual: [{}])",
                firstUncommittedSlot(), publishResponse.getSlot());
            throw new IllegalArgumentException("incoming slot " + publishResponse.getSlot() + " does not match current slot " +
                firstUncommittedSlot());
        }

        logger.trace("handlePublishResponse: accepted publish response for slot [{}] and term [{}]",
            publishResponse.getSlot(), publishResponse.getTerm());
        publishState.add(sourceNode);
        if (committedState.getVotingNodes().isQuorum(publishState)) {
            logger.trace("handlePublishResponse: value committed for slot [{}] and term [{}]",
                firstUncommittedSlot(), currentTerm);
            return Optional.of(new ApplyCommit(publishResponse.getSlot(), publishResponse.getTerm()));
        }

        return Optional.empty();
    }

    public void handleCommit(ApplyCommit applyCommit) {
        if (applyCommit.getTerm() != lastAcceptedTermInSlot()) {
            logger.trace("handleCommitRequest: ignored commit request due to term mismatch (expected: [{}], actual: [{}])",
                lastAcceptedTermInSlot(), applyCommit.getTerm());
            throw new IllegalArgumentException("incoming term " + applyCommit.getTerm() + " does not match last accepted term " +
                lastAcceptedTermInSlot());
        }
        if (applyCommit.getSlot() != firstUncommittedSlot()) {
            logger.trace("handleCommitRequest: ignored commit request due to slot mismatch (expected: [{}], actual: [{}])",
                firstUncommittedSlot(), applyCommit.getSlot());
            throw new IllegalArgumentException("incoming slot " + applyCommit.getSlot() + " does not match current slot " +
                firstUncommittedSlot());
        }

        logger.trace("handleCommitRequest: applying commit request for slot [{}]",
            applyCommit.getSlot());

        assert acceptedState.isPresent();
        assert acceptedState.get().getSlot() == committedState.getSlot() + 1;
        final T newCommittedState = acceptedState.get().getDiff().apply(committedState);
        assert newCommittedState.getSlot() == committedState.getSlot() + 1;

        persistence.persistCommittedState(newCommittedState);
        committedState = newCommittedState;
        electionState.setValueForced(false);
        publishState = new PublishState(true);
    }

    public T generateCatchup() {
        logger.trace("generateCatchup: generating catch up for slot [{}]", committedState.getSlot());
        return committedState;
    }

    public void applyCatchup(T newCommittedState) {
        if (newCommittedState.getSlot() <= committedState.getSlot()) {
            logger.trace("applyCatchup: ignored catch up request due to slot mismatch (expected: >[{}], actual: [{}])",
                committedState.getSlot(), newCommittedState.getSlot());
            throw new IllegalArgumentException("incoming slot " + newCommittedState.getSlot() + " higher than current slot " +
                committedState.getSlot());
        }

        logger.trace("applyCatchup: applying catch up for slot [{}]", newCommittedState.getSlot());
        persistence.persistCommittedState(newCommittedState);
        committedState = newCommittedState;
        electionState = new ElectionState();
        publishState = new PublishState(false);
    }

    public PublishRequest<T> handleClientValue(Diff<T> diff) {
        if (electionState.electionWon() == false) {
            logger.trace("handleClientValue: ignored request as election not won");
            throw new IllegalArgumentException("election not won");
        }
        if (publishState.publishPermitted() == false) {
            logger.trace("handleClientValue: ignored request as publishing is not permitted");
            throw new IllegalArgumentException("publishing not permitted");
        }
        if (electionState.valueForced()) {
            logger.trace("handleClientValue: ignored request as election value is forced");
            throw new IllegalArgumentException("election value forced");
        }

        logger.trace("handleClientValue: processing request for slot [{}]", firstUncommittedSlot());
        publishState.disablePublishing();
        return new PublishRequest<>(firstUncommittedSlot(), currentTerm, diff);
    }

    public static class NodeCollection {

        protected final Map<String, DiscoveryNode> nodes = new HashMap<>();

        public void add(DiscoveryNode sourceNode) {
            nodes.put(sourceNode.getId(), sourceNode);
        }

        public boolean isQuorum(NodeCollection votes) {
            final HashSet<String> intersection = new HashSet(nodes.keySet());
            intersection.retainAll(votes.nodes.keySet());
            return intersection.size() * 2 > nodes.size();
        }
    }

    public static class ElectionState extends NodeCollection {
        private boolean electionWon;
        private boolean electionValueForced;

        public ElectionState() {
            electionWon = false;
            electionValueForced = false;
        }

        public boolean valueForced() {
            return electionValueForced;
        }

        public void setValueForced(boolean forced) {
            electionValueForced = forced;
        }

        public boolean electionWon() {
            return electionWon;
        }

        public boolean maybeSetElectionWon(NodeCollection config) {
            if (electionWon() == false && config.isQuorum(this)) {
                this.electionWon = true;
                return true;
            }
            return false;
        }

        @Override
        public String toString() {
            return "ElectionState{" +
                "nodes=" + nodes +
                ", electionWon=" + electionWon +
                ", electionValueForced=" + electionValueForced +
                '}';
        }
    }

    public static class PublishState extends NodeCollection {
        private boolean publishPermitted;

        public PublishState(boolean publishPermitted) {
            this.publishPermitted = publishPermitted;
        }

        public void disablePublishing() {
            publishPermitted = true;
        }

        public boolean publishPermitted() {
            return publishPermitted;
        }

        @Override
        public String toString() {
            return "PublishState{" +
                "nodes=" + nodes +
                ", publishPermitted=" + publishPermitted +
                '}';
        }
    }

    public static class Vote {
        private final long firstUncommittedSlot;
        private final long term;
        private final long lastAcceptedTerm;

        public Vote(long firstUncommittedSlot, long term, long lastAcceptedTerm) {
            assert firstUncommittedSlot >= 0;
            assert term >= 0;
            assert lastAcceptedTerm == NO_TERM || lastAcceptedTerm >= 0;

            this.firstUncommittedSlot = firstUncommittedSlot;
            this.term = term;
            this.lastAcceptedTerm = lastAcceptedTerm;
        }

        public long getFirstUncommittedSlot() {
            return firstUncommittedSlot;
        }

        public long getTerm() {
            return term;
        }

        public long getLastAcceptedTerm() {
            return lastAcceptedTerm;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Vote vote = (Vote) o;

            if (firstUncommittedSlot != vote.firstUncommittedSlot) return false;
            if (term != vote.term) return false;
            return lastAcceptedTerm == vote.lastAcceptedTerm;
        }

        @Override
        public int hashCode() {
            int result = (int) (firstUncommittedSlot ^ (firstUncommittedSlot >>> 32));
            result = 31 * result + (int) (term ^ (term >>> 32));
            result = 31 * result + (int) (lastAcceptedTerm ^ (lastAcceptedTerm >>> 32));
            return result;
        }

        @Override
        public String toString() {
            return "Vote{" +
                "firstUncommittedSlot=" + firstUncommittedSlot +
                ", term=" + term +
                ", lastAcceptedTerm=" + lastAcceptedTerm +
                '}';
        }
    }

    public static class SlotTerm {
        protected final long slot;
        protected final long term;

        public SlotTerm(long slot, long term) {
            assert slot >= 0;
            assert term >= 0;

            this.slot = slot;
            this.term = term;
        }

        public long getSlot() {
            return slot;
        }

        public long getTerm() {
            return term;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            SlotTerm slotTerm = (SlotTerm) o;

            if (slot != slotTerm.slot) return false;
            return term == slotTerm.term;
        }

        @Override
        public int hashCode() {
            int result = (int) (slot ^ (slot >>> 32));
            result = 31 * result + (int) (term ^ (term >>> 32));
            return result;
        }

        @Override
        public String toString() {
            return "SlotTerm{" +
                "slot=" + slot +
                ", term=" + term +
                '}';
        }
    }

    public static class PublishResponse extends SlotTerm {

        public PublishResponse(long slot, long term) {
            super(slot, term);
        }

        @Override
        public String toString() {
            return "PublishResponse{" +
                "slot=" + slot +
                ", term=" + term +
                '}';
        }
    }

    public static class ApplyCommit extends SlotTerm {

        public ApplyCommit(long slot, long term) {
            super(slot, term);
        }

        @Override
        public String toString() {
            return "ApplyCommit{" +
                "slot=" + slot +
                ", term=" + term +
                '}';
        }
    }

    public static class SlotTermDiff<T> extends SlotTerm {
        protected final Diff<T> diff;

        public SlotTermDiff(long slot, long term, Diff<T> diff) {
            super(slot, term);
            this.diff = diff;
        }

        public Diff<T> getDiff() {
            return diff;
        }

        @Override
        public boolean equals(Object o) {
            if (super.equals(o) == false) { return false; }
            SlotTermDiff<?> that = (SlotTermDiff<?>) o;
            return diff != null ? diff.equals(that.diff) : that.diff == null;
        }

        @Override
        public int hashCode() {
            int result = super.hashCode();
            result = 31 * result + (diff != null ? diff.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "SlotTermDiff{" +
                "slot=" + slot +
                ", term=" + term +
                ", diff=" + diff +
                '}';
        }
    }

    public static class PublishRequest<T> extends SlotTermDiff<T> {

        public PublishRequest(long slot, long term, Diff<T> diff) {
            super(slot, term, diff);
        }

        @Override
        public String toString() {
            return "PublishRequest{" +
                "slot=" + slot +
                ", term=" + term +
                ", diff=" + diff +
                '}';
        }
    }

    public interface CommittedState {
        long getSlot();
        NodeCollection getVotingNodes();
    }

    public interface Persistence<T> {
        void persistCurrentTerm(long currentTerm);
        void persistCommittedState(T committedState);
        void persistAcceptedState(SlotTermDiff<T> slotTermDiff);
    }
}
