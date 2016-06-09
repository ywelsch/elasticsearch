package org.elasticsearch.cluster.service;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;

public class LocalNodeMasterListeners implements ClusterStateListener {

    private final List<LocalNodeMasterListener> listeners = new CopyOnWriteArrayList<>();
    private final ThreadPool threadPool;
    private volatile boolean master = false;

    public LocalNodeMasterListeners(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (!master && event.localNodeMaster()) {
            master = true;
            for (LocalNodeMasterListener listener : listeners) {
                Executor executor = threadPool.executor(listener.executorName());
                executor.execute(listener::onMaster);
            }
            return;
        }

        if (master && !event.localNodeMaster()) {
            master = false;
            for (LocalNodeMasterListener listener : listeners) {
                Executor executor = threadPool.executor(listener.executorName());
                executor.execute(listener::offMaster);
            }
        }
    }

    public void add(LocalNodeMasterListener listener) {
        listeners.add(listener);
    }

    public void remove(LocalNodeMasterListener listener) {
        listeners.remove(listener);
    }

    public void clear() {
        listeners.clear();
    }
}
