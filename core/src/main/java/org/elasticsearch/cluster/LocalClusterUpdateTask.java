package org.elasticsearch.cluster;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;

public abstract class LocalClusterUpdateTask extends ClusterStateUpdateTask {

    public LocalClusterUpdateTask() {
        super();
    }

    public LocalClusterUpdateTask(Priority priority) {
        super(priority);
    }

    public abstract LocalResult executeLocally(ClusterState currentState) throws Exception;

    @Override
    public ClusterState execute(ClusterState currentState) throws Exception {
        throw new UnsupportedOperationException("cannot run execute method on local cluster update task");
    }

    @Override
    public boolean runOnlyOnMaster() {
        return false;
    }

    public static class LocalResult {
        @Nullable
        public final boolean offMaster;
        @Nullable
        public final ClusterState clusterState;

        LocalResult(boolean offMaster, ClusterState clusterState) {
            this.offMaster = offMaster;
            this.clusterState = clusterState;
        }
    }

    public static LocalResult offMaster() {
        return new LocalResult(true, null);
    }

    public static LocalResult unchanged() {
        return new LocalResult(false, null);
    }

    public static LocalResult newState(ClusterState clusterState) {
        return new LocalResult(false, clusterState);
    }
}
