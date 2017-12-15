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

public class Messages {

    public static final long NO_TERM = -1L;

    public static class OfferVote {
        protected final long firstUncommittedSlot;
        protected final long term;
        protected final long lastAcceptedTerm;

        public OfferVote(long firstUncommittedSlot, long term, long lastAcceptedTerm) {
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

    public static class Vote {
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

    public static class PublishRequest<T> extends SlotTerm {

        protected final Diff<T> diff;

        public PublishRequest(long slot, long term, Diff<T> diff) {
            super(slot, term);
            this.diff = diff;
        }

        public Diff<T> getDiff() {
            return diff;
        }

        public ConsensusState.AcceptedState<T> getAcceptedState() {
            return new ConsensusState.AcceptedState<>(term, diff);
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
}
