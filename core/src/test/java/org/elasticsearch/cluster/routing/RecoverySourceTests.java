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

package org.elasticsearch.cluster.routing;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class RecoverySourceTests extends ESTestCase {

    public void testSerialization() throws IOException {
        RecoverySource recoverySource = TestShardRouting.randomRecoverySource();
        BytesStreamOutput out = new BytesStreamOutput();
        recoverySource.writeTo(out);
        RecoverySource serializedRecoverySource = RecoverySource.readFrom(out.bytes().streamInput());
        assertEquals(recoverySource.getType(), serializedRecoverySource.getType());
        assertEquals(recoverySource, serializedRecoverySource);
    }

    public void testRecoverySourceTypeOrder() {
        assertEquals(RecoverySource.Type.STORE.id(), (byte) 0);
        assertEquals(RecoverySource.Type.PEER.id(), (byte) 1);
        assertEquals(RecoverySource.Type.SNAPSHOT.id(), (byte) 2);
        assertEquals(RecoverySource.Type.LOCAL_SHARDS.id(), (byte) 3);
        // check exhaustiveness
        for (RecoverySource.Type type : RecoverySource.Type.values()) {
            assertThat(type.id(), greaterThanOrEqualTo((byte) 0));
            assertThat(type.id(), lessThanOrEqualTo((byte) 3));
        }
    }
}
