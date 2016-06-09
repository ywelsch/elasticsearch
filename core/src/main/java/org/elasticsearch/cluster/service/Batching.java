package org.elasticsearch.cluster.service;

import org.elasticsearch.cluster.service.ClusterTaskExecutor.SourcePrioritizedRunnable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.util.Callback;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class Batching<K, V extends Batching.BatchedSourcePrioritizedRunnable> {

    private final Map<K, List<V>> batches = new HashMap<>();
    private final ESLogger logger;

    public Batching(ESLogger logger) {
        this.logger = logger;
    }

    public static class BatchedExecution<T> {
        public final ArrayList<T> toExecute;
        public final String combinedSource;

        public BatchedExecution(ArrayList<T> toExecute, ArrayList<String> sources) {
            this.toExecute = toExecute;
            this.combinedSource = Strings.collectionToCommaDelimitedString(sources);
        }

        public boolean isEmpty() {
            return toExecute.isEmpty();
        }
    }


    public void addToBatches(K key, V value) {
        synchronized (batches) {
            batches.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
        }
    }

    public void addToBatches(K key, List<V> values, Callback<V> forExisting) {
        synchronized (batches) {
            List<V> existingTasks = batches.computeIfAbsent(key, k -> new ArrayList<>());
            for (V existing : existingTasks) {
                forExisting.handle(existing);
            }
            existingTasks.addAll(values);
        }
    }

    public BatchedExecution<V> getCurrentBatch(K key) {
        final ArrayList<V> toExecute = new ArrayList<>();
        final ArrayList<String> sources = new ArrayList<>();
        synchronized (batches) {
            List<V> pending = batches.remove(key);
            if (pending != null) {
                for (V task : pending) {
                    if (task.processed.getAndSet(true) == false) {
                        logger.trace("will process [{}]", task.source);
                        toExecute.add(task);
                        sources.add(task.source);
                    } else {
                        logger.trace("skipping [{}], already processed", task.source);
                    }
                }
            }
        }
        return new BatchedExecution<>(toExecute, sources);
    }

    public static abstract class BatchedSourcePrioritizedRunnable extends SourcePrioritizedRunnable {
        public final AtomicBoolean processed = new AtomicBoolean();

        public BatchedSourcePrioritizedRunnable(Priority priority, String source) {
            super(priority, source);
        }
    }

}
