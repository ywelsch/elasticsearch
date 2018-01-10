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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

public class Messages {

    public static final long NO_TERM = -1L;

    public static class OfferVote extends TransportResponse {
        protected final long firstUncommittedSlot;
        protected final long term;
        protected final long lastAcceptedTerm;

        public OfferVote(long firstUncommittedSlot, long term, long lastAcceptedTerm) {
            this.firstUncommittedSlot = firstUncommittedSlot;
            this.term = term;
            this.lastAcceptedTerm = lastAcceptedTerm;
        }

        public OfferVote(StreamInput in) throws IOException {
            firstUncommittedSlot = in.readLong();
            term = in.readLong();
            lastAcceptedTerm = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(firstUncommittedSlot);
            out.writeLong(term);
            out.writeLong(lastAcceptedTerm);
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
        public String toString() {
            return "OfferVote{firstUncommittedSlot=" + firstUncommittedSlot
                + ", term=" + term + ", lastAcceptedTerm=" + lastAcceptedTerm + '}';
        }
    }

    public static class SeekVotes extends SlotTerm {
        public SeekVotes(long slot, long term) {
            super(slot, term);
        }

        @Override
        public String toString() {
            return "SeekVotes{" + "slot=" + slot + ", term=" + term + '}';
        }
    }

    public static class Vote extends TransportResponse {
        protected final long firstUncommittedSlot;
        protected final long term;
        protected final long lastAcceptedTerm;

        public Vote(long firstUncommittedSlot, long term, long lastAcceptedTerm) {
            assert firstUncommittedSlot >= 0;
            assert term >= 0;
            assert lastAcceptedTerm == NO_TERM || lastAcceptedTerm >= 0;

            this.firstUncommittedSlot = firstUncommittedSlot;
            this.term = term;
            this.lastAcceptedTerm = lastAcceptedTerm;
        }

        public Vote(StreamInput in) throws IOException {
            firstUncommittedSlot = in.readLong();
            term = in.readLong();
            lastAcceptedTerm = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(firstUncommittedSlot);
            out.writeLong(term);
            out.writeLong(lastAcceptedTerm);
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

    public static class CatchupResponse<T> extends TransportResponse {

        protected final T fullState;

        public CatchupResponse(T fullState) {
            this.fullState = fullState;
        }

        public T getFullState() {
            return fullState;
        }

        @Override
        public boolean equals(Object o) {
            if (super.equals(o) == false) {
                return false;
            }
            CatchupResponse<?> that = (CatchupResponse<?>) o;
            return fullState != null ? fullState.equals(that.fullState) : that.fullState == null;
        }

        @Override
        public int hashCode() {
            int result = super.hashCode();
            result = 31 * result + (fullState != null ? fullState.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "CatchupResponse{" +
                "fullState=" + fullState + "}";
        }
    }

    public abstract static class SlotTerm {
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

    public abstract static class SlotTermResponse extends TransportResponse {
        protected final long slot;
        protected final long term;

        public SlotTermResponse(long slot, long term) {
            assert slot >= 0;
            assert term >= 0;

            this.slot = slot;
            this.term = term;
        }

        public SlotTermResponse(StreamInput in) throws IOException {
            this(in.readLong(), in.readLong());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(slot);
            out.writeLong(term);
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
            return "SlotTermResponse{" +
                "slot=" + slot +
                ", term=" + term +
                '}';
        }
    }

    public static class LegislatorPublishResponse extends TransportResponse {
        private final long firstUncommittedSlot; // first uncommitted slot on sender - could indicate that the sender is ahead
        private final Optional<PublishResponse> publishResponse; // if node accepted publish request
        private final Optional<Vote> vote; // if vote was granted due to node having lower term

        public LegislatorPublishResponse(long firstUncommittedSlot, Optional<PublishResponse> publishResponse, Optional<Vote> vote) {
            this.firstUncommittedSlot = firstUncommittedSlot;
            this.publishResponse = publishResponse;
            this.vote = vote;
        }

        public LegislatorPublishResponse(StreamInput in) throws IOException {
            this.firstUncommittedSlot = in.readLong();
            this.publishResponse = Optional.ofNullable(in.readOptionalWriteable(PublishResponse::new));
            this.vote = Optional.ofNullable(in.readOptionalWriteable(Vote::new));
        }

        public Optional<PublishResponse> getPublishResponse() {
            return publishResponse;
        }

        public Optional<Vote> getVote() {
            return vote;
        }

        public long getFirstUncommittedSlot() {
            return firstUncommittedSlot;
        }
    }

    public static class PublishResponse extends SlotTermResponse {

        public PublishResponse(long slot, long term) {
            super(slot, term);
        }

        public PublishResponse(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public String toString() {
            return "PublishResponse{" +
                "slot=" + slot +
                ", term=" + term +
                '}';
        }
    }

    public static class HeartbeatRequest extends SlotTerm {

        public HeartbeatRequest(long slot, long term) {
            super(slot, term);
        }

        @Override
        public String toString() {
            return "HeartbeatRequest{" +
                "slot=" + slot +
                ", term=" + term +
                '}';
        }
    }

    public static class HeartbeatResponse extends SlotTermResponse {

        public HeartbeatResponse(long slot, long term) {
            super(slot, term);
        }

        public HeartbeatResponse(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public String toString() {
            return "HeartbeatResponse{" +
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

    public static class CatchupRequest<T> {

        private final long term;

        private final T fullState;

        public CatchupRequest(long term, T fullState) {
            this.term = term;
            this.fullState = fullState;
        }

        public long getTerm() {
            return term;
        }

        public T getFullState() {
            return fullState;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CatchupRequest<?> that = (CatchupRequest<?>) o;
            return term == that.term &&
                Objects.equals(fullState, that.fullState);
        }

        @Override
        public int hashCode() {

            return Objects.hash(term, fullState);
        }

        @Override
        public String toString() {
            return "CatchupRequest{" +
                "term=" + term +
                ", fullState=" + fullState +
                '}';
        }
    }

    public static class PublishRequest<T> extends SlotTerm {

        protected final Diff<T> diff;

        private final ConsensusState.AcceptedState<T> acceptedState;

        public PublishRequest(long slot, long term, Diff<T> diff) {
            super(slot, term);
            this.diff = diff;
            this.acceptedState = new ConsensusState.AcceptedState<>(term, diff);
        }

        public Diff<T> getDiff() {
            return diff;
        }

        public ConsensusState.AcceptedState<T> getAcceptedState() {
            return acceptedState;
        }

        @Override
        public boolean equals(Object o) {
            if (super.equals(o) == false) {
                return false;
            }
            PublishRequest<?> that = (PublishRequest<?>) o;
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
            return "PublishRequest{" +
                "slot=" + slot +
                ", term=" + term +
                ", diff=[" + diff +
                "]}";
        }
    }

    public static class StartVoteRequest extends TransportRequest {

        private final long term;

        public StartVoteRequest(long term) {
            this.term = term;
        }

        public StartVoteRequest(StreamInput input) throws IOException {
            super(input);
            this.term = input.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeLong(term);
        }

        public long getTerm() {
            return term;
        }

        @Override
        public String toString() {
            return "StartVoteRequest{" +
                "term=" + term + "}";
        }

    }
}
