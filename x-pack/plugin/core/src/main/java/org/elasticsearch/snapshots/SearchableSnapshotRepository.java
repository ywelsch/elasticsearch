package org.elasticsearch.snapshots;

import org.apache.lucene.store.Directory;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.FrozenEngine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardRestoreFailedException;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.repositories.FilterRepository;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.Repository;

import java.io.IOException;
import java.util.function.Function;

public class SearchableSnapshotRepository extends FilterRepository {

    public static final Setting<Boolean> SEARCHABLE_SNAPSHOTS = Setting.boolSetting("index.searchable_snapshots.enabled", false, Setting
        .Property.IndexScope, Setting.Property.Final, Setting.Property.PrivateIndex);
    public static final Setting<String> SEARCHABLE_SNAPSHOTS_REPOSITORY_NAME = Setting.simpleString("index.searchable_snapshots.repository_name", Setting
        .Property.IndexScope, Setting.Property.Final, Setting.Property.PrivateIndex);
    public static final Setting<String> SEARCHABLE_SNAPSHOTS_SNAPSHOT_NAME = Setting.simpleString("index.searchable_snapshots.snapshot_name", Setting
        .Property.IndexScope, Setting.Property.Final, Setting.Property.PrivateIndex);
    public static final Setting<String> SEARCHABLE_SNAPSHOTS_SNAPSHOT_UUID = Setting.simpleString("index.searchable_snapshots.snapshot_uuid", Setting
        .Property.IndexScope, Setting.Property.Final, Setting.Property.PrivateIndex);
    public static final Setting<String> SEARCHABLE_SNAPSHOTS_INDEX_NAME = Setting.simpleString("index.searchable_snapshots.index_name", Setting
        .Property.IndexScope, Setting.Property.Final, Setting.Property.PrivateIndex);
    public static final Setting<String> SEARCHABLE_SNAPSHOTS_INDEX_ID = Setting.simpleString("index.searchable_snapshots.index_id", Setting
        .Property.IndexScope, Setting.Property.Final, Setting.Property.PrivateIndex);
    private static final Setting<String> DELEGATE_TYPE = new Setting<>("delegate_type", "", Function.identity(), Setting.Property
        .NodeScope);

    public SearchableSnapshotRepository(Repository in) {
        super(in);
    }

    @Override
    public IndexMetaData getSnapshotIndexMetaData(SnapshotId snapshotId, IndexId index) throws IOException {
        final IndexMetaData originalIndexMetaData = super.getSnapshotIndexMetaData(snapshotId, index);
        return IndexMetaData.builder(originalIndexMetaData)
            .settings(Settings.builder().put(originalIndexMetaData.getSettings())
                .put(SEARCHABLE_SNAPSHOTS.getKey(), true)
                .put(IndexSettings.INDEX_SEARCH_THROTTLED.getKey(), true)
                // peer recovery and other bits are broken, so force single primary that can't be moved either
                .put("index.number_of_replicas", 0)
                .put(EnableAllocationDecider.INDEX_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.NONE)
                .put(SEARCHABLE_SNAPSHOTS_REPOSITORY_NAME.getKey(), getMetadata().name())
                .put(SEARCHABLE_SNAPSHOTS_SNAPSHOT_NAME.getKey(), snapshotId.getName())
                .put(SEARCHABLE_SNAPSHOTS_SNAPSHOT_UUID.getKey(), snapshotId.getUUID())
                .put(SEARCHABLE_SNAPSHOTS_INDEX_NAME.getKey(), index.getName())
                .put(SEARCHABLE_SNAPSHOTS_INDEX_ID.getKey(), index.getId())
            )
            .build();
    }

    @Override
    public void restoreShard(IndexShard shard, SnapshotId snapshotId, Version version, IndexId indexId, ShardId snapshotShardId,
                             RecoveryState recoveryState) {
        // restore an empty shard
        final Store store = shard.store();
        store.incRef();
        try {
            store.createEmpty(shard.indexSettings().getIndexMetaData().getCreationVersion().luceneVersion);
        } catch (IOException e) {
            throw new IndexShardRestoreFailedException(shard.shardId(), "failed to restore snapshot [" + snapshotId + "]", e);
        } finally {
            store.decRef();
        }
    }

    public static EngineFactory getEngineFactory() {
        return config -> {
            String repoName = config.getIndexSettings().getValue(SEARCHABLE_SNAPSHOTS_REPOSITORY_NAME);
            String snapshotName = config.getIndexSettings().getValue(SEARCHABLE_SNAPSHOTS_SNAPSHOT_NAME);
            String snapshotUUID = config.getIndexSettings().getValue(SEARCHABLE_SNAPSHOTS_SNAPSHOT_UUID);
            String indexName = config.getIndexSettings().getValue(SEARCHABLE_SNAPSHOTS_INDEX_NAME);
            String indexId = config.getIndexSettings().getValue(SEARCHABLE_SNAPSHOTS_INDEX_ID);
            SnapshotId snapshotId = new SnapshotId(snapshotName, snapshotUUID);
            IndexId index = new IndexId(indexName, indexId);
            Repository repository = config.repositoriesService().repository(repoName);

            Directory directory = repository.asDirectory(snapshotId, index, config.getShardId());
            return new FrozenEngine(config) {

                @Override
                protected Directory getDirectoryForReadOnlyOperations() {
                    return directory;
                }

                @Override
                protected void assertMaxSeqNoEqualsToGlobalCheckpoint(final long maxSeqNo, final long globalCheckpoint) {
                    // NOOP
                }
            };
        };
    }


    /**
     * Returns a new source only repository factory
     */
    public static Repository.Factory newRepositoryFactory() {
        return new Repository.Factory() {

            @Override
            public Repository create(RepositoryMetaData metadata) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Repository create(RepositoryMetaData metaData, Function<String, Repository.Factory> typeLookup) throws Exception {
                String delegateType = DELEGATE_TYPE.get(metaData.settings());
                if (Strings.hasLength(delegateType) == false) {
                    throw new IllegalArgumentException(DELEGATE_TYPE.getKey() + " must be set");
                }
                Repository.Factory factory = typeLookup.apply(delegateType);
                return new SearchableSnapshotRepository(factory.create(new RepositoryMetaData(metaData.name(),
                    delegateType, metaData.settings()), typeLookup));
            }
        };
    }
}
