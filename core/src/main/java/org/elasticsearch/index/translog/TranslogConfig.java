package org.elasticsearch.index.translog;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;

import java.nio.file.Path;

public class TranslogConfig extends BaseTranslogConfig {

    private final IndexSettings indexSettings;
    private final ShardId shardId;

    public TranslogConfig(ShardId shardId, Path translogPath, IndexSettings indexSettings, BigArrays bigArrays) {
        this(shardId, translogPath, indexSettings, bigArrays, DEFAULT_BUFFER_SIZE);
    }

    public TranslogConfig(ShardId shardId, Path translogPath, IndexSettings indexSettings, BigArrays bigArrays,
                          ByteSizeValue bufferSize) {
        super(translogPath, indexSettings.getNodeSettings(), bigArrays, bufferSize);
        this.indexSettings = indexSettings;
        this.shardId = shardId;
    }

    /**
     * Returns the index indexSettings
     */
    public IndexSettings getIndexSettings() {
        return indexSettings;
    }

    /**
     * Returns the shard ID this config is created for
     */
    public ShardId getShardId() {
        return shardId;
    }

}
