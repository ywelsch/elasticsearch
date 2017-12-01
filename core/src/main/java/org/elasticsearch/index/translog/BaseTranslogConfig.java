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

package org.elasticsearch.index.translog;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;

import java.nio.file.Path;

/*
 * Holds all the configuration that is used to create a {@link BaseTranslog}.
 * Once {@link BaseTranslog} has been created with this object, changes to this
 * object will affect the {@link BaseTranslog} instance.
 */
public class BaseTranslogConfig {

    public static final ByteSizeValue DEFAULT_BUFFER_SIZE = new ByteSizeValue(8, ByteSizeUnit.KB);
    private final BigArrays bigArrays;
    private final Path translogPath;
    private final ByteSizeValue bufferSize;
    private final Settings settings;

    /**
     * Creates a new BaseTranslogConfig instance
     * @param translogPath the path to use for the transaction log files
     * @param bigArrays a bigArrays instance used for temporarily allocating write operations
     */
    public BaseTranslogConfig(Path translogPath, Settings settings, BigArrays bigArrays, ByteSizeValue bufferSize) {
        this.bufferSize = bufferSize;
        this.translogPath = translogPath;
        this.settings = settings;
        this.bigArrays = bigArrays;
    }

    /**
     * Returns a BigArrays instance for this engine
     */
    public BigArrays getBigArrays() {
        return bigArrays;
    }

    /**
     * Returns the translog path for this engine
     */
    public Path getTranslogPath() {
        return translogPath;
    }

    /**
     * The translog buffer size. Default is <tt>8kb</tt>
     */
    public ByteSizeValue getBufferSize() {
        return bufferSize;
    }

    /**
     * The node settings
     */
    public Settings getSettings() {
        return settings;
    }

}
