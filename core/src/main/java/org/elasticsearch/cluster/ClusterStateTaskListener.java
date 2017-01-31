package org.elasticsearch.cluster;

import java.util.List;

public interface ClusterStateTaskListener {

    /**
     * A callback called when execute fails.
     */
    void onFailure(String source, Exception e);

    /**
     * Called when the result of the {@link ClusterStateTaskExecutor#execute(ClusterState, List)} have been processed
     * properly by all listeners.
     */
    default void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
    }
}
