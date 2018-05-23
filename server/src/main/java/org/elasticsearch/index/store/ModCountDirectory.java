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

package org.elasticsearch.index.store;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.common.lucene.store.FilterIndexOutput;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

final class ModCountDirectory extends FilterDirectory {

    private final AtomicLong modCount = new AtomicLong();

    ModCountDirectory(Directory in) {
        super(in);
    }

    public long getModCount() {
        return modCount.get();
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        return wrapIndexOutput(super.createOutput(name, context));
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        return wrapIndexOutput(super.createTempOutput(prefix, suffix, context));
    }

    private IndexOutput wrapIndexOutput(IndexOutput out) {
        return new FilterIndexOutput(out.toString(), out) {
            @Override
            public void writeBytes(byte[] b, int length) throws IOException {
                super.writeBytes(b, length);
                modCount.incrementAndGet();
            }

            @Override
            public void writeByte(byte b) throws IOException {
                super.writeByte(b);
                modCount.incrementAndGet();
            }

            @Override
            public void close() throws IOException {
                // Close might cause some data to be flushed from in-memory buffers, so
                // increment the modification counter too.
                super.close();
                modCount.incrementAndGet();
            }
        };
    }

    @Override
    public void deleteFile(String name) throws IOException {
        super.deleteFile(name);
        modCount.incrementAndGet();
    }

}
