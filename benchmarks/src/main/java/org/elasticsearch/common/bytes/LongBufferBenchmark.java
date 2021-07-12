/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.bytes;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.util.concurrent.TimeUnit;

@Warmup(iterations = 1)
@Measurement(iterations = 3)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Fork(value = 1)
public class LongBufferBenchmark {

    @Param(value = { "16384" })
    private int bufSize;

    @Param(value = { "1000" })
    private int readIters;

    private long[] longArray;

    private byte[] byteArray;

    private LongBuffer simpleLongBuffer;

    private LongBuffer wrappedLongBuffer;

    private LongBuffer wrappedNativeEndianLongBuffer;

    @Setup
    public void initResults() {
        longArray = new long[bufSize / 8];
        for (int i = 0; i < longArray.length; i++) {
            longArray[i] = i;
        }
        byteArray = new byte[bufSize];
        for (int i = 0; i < byteArray.length; i+=8) {
            setLong(byteArray, i, i);
        }
        simpleLongBuffer = LongBuffer.allocate(bufSize / 8);
        fillBuffer(simpleLongBuffer);
        wrappedLongBuffer = ByteBuffer.allocate(bufSize).asLongBuffer();
        fillBuffer(wrappedLongBuffer);
        assert ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;
        wrappedNativeEndianLongBuffer = ByteBuffer.allocate(bufSize).order(ByteOrder.nativeOrder()).asLongBuffer();
        fillBuffer(wrappedNativeEndianLongBuffer);
    }

    private static void fillBuffer(LongBuffer longBuffer) {
        for (int i = 0; i < longBuffer.capacity(); i++) {
            longBuffer.put(i, i);
        }
    }

    @Benchmark
    public long readLongArray() {
        long res = 0L;
        for (int j = 0; j < readIters; j++) {
            for (int i = 0; i < longArray.length; i++) {
                res = res ^ longArray[i];
            }
        }
        return res;
    }

    @Benchmark
    public long readByteArray() {
        long res = 0L;
        int length = byteArray.length / 8;
        for (int j = 0; j < readIters; j++) {
            for (int i = 0; i < length; i++) {
                res = res ^ getLong(byteArray, i << 3);
            }
        }
        return res;
    }

    @Benchmark
    public long readSimpleLongBuffer() {
        return readLong(readIters, simpleLongBuffer);
    }

    @Benchmark
    public long readLongBufferWrappedByteBuffer() {
        return readLong(readIters, wrappedLongBuffer);
    }

    @Benchmark
    public long readLongBufferWrappedNativeEndianByteBuffer() {
        return readLong(readIters, wrappedNativeEndianLongBuffer);
    }

    private static long readLong(int readIters, LongBuffer longBuffer) {
        long res = 0L;
        for (int j = 0; j < readIters; j++) {
            for (int i = 0; i < longBuffer.capacity(); i++) {
                res = res ^ longBuffer.get(i);
            }
        }
        return res;
    }

    static void setLong(byte[] memory, int index, long value) {
        memory[0 + index] = (byte) (value >> 56);
        memory[1 + index] = (byte) (value >> 48);
        memory[2 + index] = (byte) (value >> 40);
        memory[3 + index] = (byte) (value >> 32);
        memory[4 + index] = (byte) (value >> 24);
        memory[5 + index] = (byte) (value >> 16);
        memory[6 + index] = (byte) (value >> 8);
        memory[7 + index] = (byte) value;
    }

    static long getLong(byte[] memory, int index) {
        return  (long) (memory[index]     & 0xff) << 56 |
            (long) (memory[index + 1] & 0xff) << 48 |
            (long) (memory[index + 2] & 0xff) << 40 |
            (long) (memory[index + 3] & 0xff) << 32 |
            (long) (memory[index + 4] & 0xff) << 24 |
            (memory[index + 5] & 0xff) << 16 |
            (memory[index + 6] & 0xff) <<  8 |
            memory[index + 7] & 0xff;
    }

}
