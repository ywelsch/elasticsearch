/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.search.stats;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Records which fields are being accessed by searches, aggregations, etc.
 */
public class ShardFieldUsageStats {

    private final Map<String, InternalFieldStats> perFieldStats = new ConcurrentHashMap<>();

    /**
     * Called whenever a query is used
     */
    public void onQuery(Query query) {
        query.visit(new QueryVisitor() {
            @Override
            public boolean acceptField(String field) {
                perFieldStats.computeIfAbsent(field, f -> new InternalFieldStats()).queryCount.incrementAndGet();
                return true;
            }

            @Override
            public void visitLeaf(Query query) {

            }
        });
    }

    /**
     * Called whenever a field is used in an aggregation context
     */
    public void onFieldAggregation(String field) {
        perFieldStats.computeIfAbsent(field, f -> new InternalFieldStats()).aggregationCount.incrementAndGet();
    }

    /**
     * Clears the currently recorded stats
     */
    public void clear() {
        perFieldStats.clear();
    }

    static class InternalFieldStats {
        final AtomicLong queryCount = new AtomicLong();
        final AtomicLong aggregationCount = new AtomicLong();
    }

    public static class FieldStats {
        public FieldStats(long queryCount, long aggregationCount) {
            this.queryCount = queryCount;
            this.aggregationCount = aggregationCount;
        }

        /**
         * Returns the number of times the given field was used in a query context
         */
        public long getQueryCount() {
            return queryCount;
        }

        private final long queryCount;

        /**
         * Returns the number of times the given field was used in an aggregation context
         */
        public long getAggregationCount() {
            return aggregationCount;
        }

        private final long aggregationCount;
    }

    /**
     * Returns an immutable snapshot of the stats
     */
    public Map<String, FieldStats> getPerFieldStats() {
        final Map<String, FieldStats> stats = new HashMap<>(perFieldStats.size());
        for (Map.Entry<String, InternalFieldStats> entry : perFieldStats.entrySet()) {
            stats.put(entry.getKey(), new FieldStats(entry.getValue().queryCount.get(), entry.getValue().aggregationCount.get()));
        }
        return Collections.unmodifiableMap(stats);
    }
}
