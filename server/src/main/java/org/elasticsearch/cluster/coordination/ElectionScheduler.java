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

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.TransportService;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class ElectionScheduler extends AbstractComponent {

    /*
     * It's provably impossible to guarantee that any leader election algorithm ever elects a leader, but they generally work (with
     * probability that approaches 1 over time) as long as elections occur sufficiently infrequently, compared to the time it takes to send
     * a message to another node and receive a response back. We do not know the round-trip latency here, but we can approximate it by
     * attempting elections randomly at reasonably high frequency and backing off (linearly) until one of them succeeds. We also place an
     * upper bound on the backoff so that if elections are failing due to a network partition that lasts for a long time then when the
     * partition heals there is an election attempt reasonably quickly.
     */

    // bounds on the time between election attempts
    private static final String ELECTION_MIN_TIMEOUT_SETTING_KEY = "cluster.election.min_timeout";
    private static final String ELECTION_BACK_OFF_TIME_SETTING_KEY = "cluster.election.back_off_time";
    private static final String ELECTION_MAX_TIMEOUT_SETTING_KEY = "cluster.election.max_timeout";

    public static final Setting<TimeValue> ELECTION_MIN_TIMEOUT_SETTING = Setting.timeSetting(ELECTION_MIN_TIMEOUT_SETTING_KEY,
        TimeValue.timeValueMillis(100), TimeValue.timeValueMillis(1), Property.NodeScope);

    public static final Setting<TimeValue> ELECTION_BACK_OFF_TIME_SETTING = Setting.timeSetting(ELECTION_BACK_OFF_TIME_SETTING_KEY,
        TimeValue.timeValueMillis(100), TimeValue.timeValueMillis(1), Property.NodeScope);

    public static final Setting<TimeValue> ELECTION_MAX_TIMEOUT_SETTING = Setting.timeSetting(ELECTION_MAX_TIMEOUT_SETTING_KEY,
        TimeValue.timeValueSeconds(10), TimeValue.timeValueMillis(200), Property.NodeScope);

    static String validationExceptionMessage(final String electionMinTimeout, final String electionMaxTimeout) {
        return new ParameterizedMessage(
            "Invalid election retry timeouts: [{}] is [{}] and [{}] is [{}], but [{}] should be at least 100ms longer than [{}]",
            ELECTION_MIN_TIMEOUT_SETTING_KEY, electionMinTimeout,
            ELECTION_MAX_TIMEOUT_SETTING_KEY, electionMaxTimeout,
            ELECTION_MAX_TIMEOUT_SETTING_KEY, ELECTION_MIN_TIMEOUT_SETTING_KEY).getFormattedMessage();
    }

    private final TimeValue minTimeout;
    private final TimeValue backoffTime;
    private final TimeValue maxTimeout;
    private final Random random;
    private final TransportService transportService;
    private final AtomicLong idSupplier = new AtomicLong();

    private volatile Object currentScheduler; // only care about its identity

    public ElectionScheduler(Settings settings, Random random, TransportService transportService) {
        super(settings);

        this.random = random;
        this.transportService = transportService;

        minTimeout = ELECTION_MIN_TIMEOUT_SETTING.get(settings);
        backoffTime = ELECTION_BACK_OFF_TIME_SETTING.get(settings);
        maxTimeout = ELECTION_MAX_TIMEOUT_SETTING.get(settings);

        if (maxTimeout.millis() < minTimeout.millis() + 100) {
            throw new IllegalArgumentException(validationExceptionMessage(minTimeout.toString(), maxTimeout.toString()));
        }
    }

    /**
     * Starts the election scheduler, which will invoke the given runnable after a randomized election timeout
     *
     * @param gracePeriod an initial grace period
     * @param scheduledRunnable the runnable to invoke after the election timeout
     */
    public void start(TimeValue gracePeriod, Runnable scheduledRunnable) {
        final ActiveScheduler currentScheduler;
        assert this.currentScheduler == null;
        this.currentScheduler = currentScheduler = new ActiveScheduler(idSupplier.incrementAndGet(), scheduledRunnable);
        currentScheduler.scheduleNextElection(gracePeriod);
    }

    public void stop() {
        assert currentScheduler != null : currentScheduler;
        logger.debug("stopping {}", currentScheduler);
        currentScheduler = null;
    }

    @SuppressForbidden(reason = "Argument to Math.abs() is definitely not Long.MIN_VALUE")
    private static long nonNegative(long n) {
        return n == Long.MIN_VALUE ? 0 : Math.abs(n);
    }

    /**
     * @param upperBound exclusive upper bound
     */
    private long randomPositiveLongLessThan(long upperBound) {
        assert 1 < upperBound : upperBound;
        return nonNegative(random.nextLong()) % (upperBound - 1) + 1;
    }

    private long backOffCurrentMaxDelay(long currentMaxDelayMillis) {
        return Math.min(maxTimeout.getMillis(), currentMaxDelayMillis + backoffTime.getMillis());
    }

    @Override
    public String toString() {
        return "ElectionScheduler{" +
            "minTimeout=" + minTimeout +
            ", maxTimeout=" + maxTimeout +
            ", backoffTime=" + backoffTime +
            ", currentScheduler=" + currentScheduler +
            '}';
    }

    private class ActiveScheduler {
        private final AtomicLong currentMaxDelayMillis = new AtomicLong(minTimeout.millis());
        private final long schedulerId;
        private final Runnable scheduledRunnable;

        ActiveScheduler(long schedulerId, Runnable scheduledRunnable) {
            this.schedulerId = schedulerId;
            this.scheduledRunnable = scheduledRunnable;
        }

        boolean isRunning() {
            return this == currentScheduler;
        }

        void scheduleNextElection(TimeValue gracePeriod) {
            final long delay;
            if (isRunning() == false) {
                logger.debug("{} not scheduling election", this);
                return;
            }

            long maxDelayMillis = this.currentMaxDelayMillis.getAndUpdate(ElectionScheduler.this::backOffCurrentMaxDelay);
            delay = randomPositiveLongLessThan(maxDelayMillis + 1) + gracePeriod.millis();
            logger.debug("{} scheduling election with delay [{}ms] (grace={}, min={}, backoff={}, current={}ms, max={})",
                this, delay, gracePeriod, minTimeout, backoffTime, maxDelayMillis, maxTimeout);

            transportService.getThreadPool().schedule(TimeValue.timeValueMillis(delay), Names.GENERIC, new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    logger.debug("unexpected exception in wakeup", e);
                    assert false : e;
                }

                @Override
                protected void doRun() {
                    if (isRunning() == false) {
                        logger.debug("{} not starting election", ActiveScheduler.this);
                        return;
                    }
                    logger.debug("{} starting runnable", ActiveScheduler.this);
                    scheduledRunnable.run();
                }

                @Override
                public void onAfter() {
                    scheduleNextElection(TimeValue.ZERO);
                }

                @Override
                public String toString() {
                    return "scheduleNextElection[" + ActiveScheduler.this + "]";
                }

                @Override
                public boolean isForceExecution() {
                    // There are very few of these scheduled, and they back off, but it's important that they're not rejected as
                    // this could prevent a cluster from ever forming.
                    return true;
                }
            });
        }

        @Override
        public String toString() {
            return "ActiveScheduler[" + schedulerId + ", currentMaxDelayMillis=" + currentMaxDelayMillis + "]";
        }
    }
}
