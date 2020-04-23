/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CompoundFormat;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.codecs.NormsConsumer;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.PointsWriter;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.codecs.TermVectorsFormat;
import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.codecs.TermVectorsWriter;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.Terms;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.CheckedSupplier;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Iterator;

/**
 * Codec that wraps another existing codec, and that delays loading segment files until required. Only supports reading.
 */
public class LazyReadOnlyCodec extends Codec {

    private final Codec delegate;

    private final LazyStoredFieldsFormat storedFieldsFormat;
    private final LazyDocValuesFormat docValuesFormat;
    private final LazyPostingsFormat postingsFormat;
    private final LazyLiveDocsFormat liveDocsFormat;
    private final LazyNormsFormat normsFormat;
    private final LazyTermVectorsFormat termVectorsFormat;
    private final LazyPointsFormat pointsFormat;

    public LazyReadOnlyCodec(String name, Codec delegate) {
        super(name);
        this.delegate = delegate;
        storedFieldsFormat = new LazyStoredFieldsFormat(delegate.storedFieldsFormat());
        docValuesFormat = new LazyDocValuesFormat(delegate.docValuesFormat());
        postingsFormat = new LazyPostingsFormat(delegate.postingsFormat());
        liveDocsFormat = new LazyLiveDocsFormat(delegate.liveDocsFormat());
        normsFormat = new LazyNormsFormat(delegate.normsFormat());
        termVectorsFormat = new LazyTermVectorsFormat(delegate.termVectorsFormat());
        pointsFormat = new LazyPointsFormat(delegate.pointsFormat());
    }

    @Override
    public StoredFieldsFormat storedFieldsFormat() {
        return storedFieldsFormat;
    }

    @Override
    public DocValuesFormat docValuesFormat() {
        return docValuesFormat;
    }

    @Override
    public PostingsFormat postingsFormat() {
        return postingsFormat;
    }

    @Override
    public LiveDocsFormat liveDocsFormat() {
        return liveDocsFormat;
    }

    @Override
    public NormsFormat normsFormat() {
        return normsFormat;
    }

    @Override
    public FieldInfosFormat fieldInfosFormat() {
        // no point in making this one lazy, its fields will be inquired at load time anyhow
        return delegate.fieldInfosFormat();
    }

    @Override
    public SegmentInfoFormat segmentInfoFormat() {
        // no point in making this one lazy, its fields will be inquired at load time anyhow
        return delegate.segmentInfoFormat();
    }

    @Override
    public TermVectorsFormat termVectorsFormat() {
        return termVectorsFormat;
    }

    @Override
    public CompoundFormat compoundFormat() {
        // no point in making this one lazy, its fields will be inquired at load time anyhow
        return delegate.compoundFormat();
    }

    @Override
    public PointsFormat pointsFormat() {
        return pointsFormat;
    }

    private static class LazyStoredFieldsFormat extends StoredFieldsFormat {

        final StoredFieldsFormat delegate;

        LazyStoredFieldsFormat(StoredFieldsFormat delegate) {
            this.delegate = delegate;
        }

        @Override
        public StoredFieldsReader fieldsReader(Directory directory, SegmentInfo si, FieldInfos fn, IOContext context) {
            return new LazyStoredFieldsReader(() -> delegate.fieldsReader(directory, si, fn, context));
        }

        @Override
        public StoredFieldsWriter fieldsWriter(Directory directory, SegmentInfo si, IOContext context) {
            throw new UnsupportedOperationException();
        }
    }

    private static class LazyStoredFieldsReader extends StoredFieldsReader {

        private final CheckedSupplier<StoredFieldsReader, IOException> storedFieldsReaderSupplier;
        private final Object mutex = new Object();
        private volatile StoredFieldsReader reader = null;

        LazyStoredFieldsReader(CheckedSupplier<StoredFieldsReader, IOException> storedFieldsReaderSupplier) {
            this.storedFieldsReaderSupplier = storedFieldsReaderSupplier;
        }

        @Override
        public void visitDocument(int docID, StoredFieldVisitor visitor) throws IOException {
            force().visitDocument(docID, visitor);
        }

        StoredFieldsReader force() throws IOException {
            if (reader == null) {
                synchronized (mutex) {
                    if (reader == null) {
                        reader = storedFieldsReaderSupplier.get();
                    }
                }
            }
            return reader;
        }

        @Override
        public StoredFieldsReader clone() {
            final StoredFieldsReader reader = this.reader;
            if (reader != null) {
                return reader.clone();
            } else {
                return new LazyStoredFieldsReader(() -> force().clone());
            }
        }

        @Override
        public void checkIntegrity() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() throws IOException {
            final StoredFieldsReader reader = this.reader;
            if (reader != null) {
                reader.close();
            }
        }

        @Override
        public long ramBytesUsed() {
            final StoredFieldsReader reader = this.reader;
            if (reader == null) {
                return 0L;
            } else {
                return reader.ramBytesUsed();
            }
        }
    }

    private static class LazyDocValuesFormat extends DocValuesFormat {

        final DocValuesFormat delegate;

        LazyDocValuesFormat(DocValuesFormat delegate) {
            super(delegate.getName());
            this.delegate = delegate;
        }

        @Override
        public DocValuesConsumer fieldsConsumer(SegmentWriteState state) {
            throw new UnsupportedOperationException();
        }

        @Override
        public DocValuesProducer fieldsProducer(SegmentReadState state) {
            return new LazyDocValuesProducer(() -> delegate.fieldsProducer(state));
        }
    }

    private static class LazyDocValuesProducer extends DocValuesProducer {

        private final CheckedSupplier<DocValuesProducer, IOException> docValuesProducerSupplier;
        private final Object mutex = new Object();
        private volatile DocValuesProducer producer = null;

        LazyDocValuesProducer(CheckedSupplier<DocValuesProducer, IOException> docValuesProducerSupplier) {
            this.docValuesProducerSupplier = docValuesProducerSupplier;
        }

        DocValuesProducer force() throws IOException {
            if (producer == null) {
                synchronized (mutex) {
                    if (producer == null) {
                        producer = docValuesProducerSupplier.get();
                    }
                }
            }
            return producer;
        }

        @Override
        public void close() throws IOException {
            final DocValuesProducer consumer = this.producer;
            if (consumer != null) {
                consumer.close();
            }
        }

        @Override
        public long ramBytesUsed() {
            final DocValuesProducer producer = this.producer;
            if (producer == null) {
                return 0L;
            } else {
                return producer.ramBytesUsed();
            }
        }

        @Override
        public NumericDocValues getNumeric(FieldInfo field) throws IOException {
            return force().getNumeric(field);
        }

        @Override
        public BinaryDocValues getBinary(FieldInfo field) throws IOException {
            return force().getBinary(field);
        }

        @Override
        public SortedDocValues getSorted(FieldInfo field) throws IOException {
            return force().getSorted(field);
        }

        @Override
        public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
            return force().getSortedNumeric(field);
        }

        @Override
        public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
            return force().getSortedSet(field);
        }

        @Override
        public void checkIntegrity() {
            throw new UnsupportedOperationException();
        }
    }

    private static class LazyPostingsFormat extends PostingsFormat {

        final PostingsFormat delegate;

        LazyPostingsFormat(PostingsFormat delegate) {
            super(delegate.getName());
            this.delegate = delegate;
        }

        @Override
        public FieldsConsumer fieldsConsumer(SegmentWriteState state) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FieldsProducer fieldsProducer(SegmentReadState state) {
            return new LazyFieldsProducer(() -> delegate.fieldsProducer(state));
        }
    }

    private static class LazyFieldsProducer extends FieldsProducer {

        private final CheckedSupplier<FieldsProducer, IOException> fieldsProducerSupplier;
        private final Object mutex = new Object();
        private volatile FieldsProducer producer = null;

        LazyFieldsProducer(CheckedSupplier<FieldsProducer, IOException> fieldsProducerSupplier) {
            this.fieldsProducerSupplier = fieldsProducerSupplier;
        }

        FieldsProducer force() throws IOException {
            if (producer == null) {
                synchronized (mutex) {
                    if (producer == null) {
                        producer = fieldsProducerSupplier.get();
                    }
                }
            }
            return producer;
        }

        @Override
        public void close() throws IOException {
            final FieldsProducer producer = this.producer;
            if (producer != null) {
                producer.close();
            }
        }

        @Override
        public long ramBytesUsed() {
            final FieldsProducer producer = this.producer;
            if (producer == null) {
                return 0L;
            } else {
                return producer.ramBytesUsed();
            }
        }

        @Override
        public void checkIntegrity() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterator<String> iterator() {
            try {
                return force().iterator();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public Terms terms(String field) throws IOException {
            return force().terms(field);
        }

        @Override
        public int size() {
            try {
                return force().size();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private static class LazyLiveDocsFormat extends LiveDocsFormat {

        final LiveDocsFormat delegate;

        LazyLiveDocsFormat(LiveDocsFormat delegate) {
            this.delegate = delegate;
        }

        @Override
        public Bits readLiveDocs(Directory dir, SegmentCommitInfo info, IOContext context) {
            return new LazyBits(() -> delegate.readLiveDocs(dir, info, context));
        }

        @Override
        public void writeLiveDocs(Bits bits, Directory dir, SegmentCommitInfo info, int newDelCount, IOContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void files(SegmentCommitInfo info, Collection<String> files) throws IOException {
            delegate.files(info, files);
        }
    }

    private static class LazyBits implements Bits {

        private final CheckedSupplier<Bits, IOException> bitsSupplier;
        private final Object mutex = new Object();
        private volatile Bits bits = null;

        LazyBits(CheckedSupplier<Bits, IOException> bitsSupplier) {
            this.bitsSupplier = bitsSupplier;
        }

        Bits force() throws IOException {
            if (bits == null) {
                synchronized (mutex) {
                    if (bits == null) {
                        bits = bitsSupplier.get();
                    }
                }
            }
            return bits;
        }

        @Override
        public boolean get(int index) {
            try {
                return force().get(index);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public int length() {
            try {
                return force().length();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private static class LazyNormsFormat extends NormsFormat {

        final NormsFormat delegate;

        LazyNormsFormat(NormsFormat delegate) {
            this.delegate = delegate;
        }

        @Override
        public NormsConsumer normsConsumer(SegmentWriteState state) {
            throw new UnsupportedOperationException();
        }

        @Override
        public NormsProducer normsProducer(SegmentReadState state) {
            return new LazyNormsProducer(() -> delegate.normsProducer(state));
        }
    }

    private static class LazyNormsProducer extends NormsProducer {

        private final CheckedSupplier<NormsProducer, IOException> normsProducerSupplier;
        private final Object mutex = new Object();
        private volatile NormsProducer normsProducer = null;

        LazyNormsProducer(CheckedSupplier<NormsProducer, IOException> normsProducerSupplier) {
            this.normsProducerSupplier = normsProducerSupplier;
        }

        NormsProducer force() throws IOException {
            if (normsProducer == null) {
                synchronized (mutex) {
                    if (normsProducer == null) {
                        normsProducer = normsProducerSupplier.get();
                    }
                }
            }
            return normsProducer;
        }

        @Override
        public NumericDocValues getNorms(FieldInfo field) throws IOException {
            return force().getNorms(field);
        }

        @Override
        public void checkIntegrity() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() throws IOException {
            final NormsProducer producer = this.normsProducer;
            if (producer != null) {
                producer.close();
            }
        }

        @Override
        public long ramBytesUsed() {
            final NormsProducer producer = this.normsProducer;
            if (producer == null) {
                return 0L;
            } else {
                return producer.ramBytesUsed();
            }
        }
    }

    private static class LazyTermVectorsFormat extends TermVectorsFormat {

        final TermVectorsFormat delegate;

        LazyTermVectorsFormat(TermVectorsFormat delegate) {
            this.delegate = delegate;
        }

        @Override
        public TermVectorsReader vectorsReader(Directory directory, SegmentInfo segmentInfo, FieldInfos fieldInfos, IOContext context) {
            return new LazyTermVectorsReader(() -> delegate.vectorsReader(directory, segmentInfo, fieldInfos, context));
        }

        @Override
        public TermVectorsWriter vectorsWriter(Directory directory, SegmentInfo segmentInfo, IOContext context) {
            throw new UnsupportedOperationException();
        }
    }

    private static class LazyTermVectorsReader extends TermVectorsReader {

        private final CheckedSupplier<TermVectorsReader, IOException> termVectorsReaderSupplier;
        private final Object mutex = new Object();
        private volatile TermVectorsReader reader = null;

        LazyTermVectorsReader(CheckedSupplier<TermVectorsReader, IOException> termVectorsReaderSupplier) {
            this.termVectorsReaderSupplier = termVectorsReaderSupplier;
        }

        TermVectorsReader force() throws IOException {
            if (reader == null) {
                synchronized (mutex) {
                    if (reader == null) {
                        reader = termVectorsReaderSupplier.get();
                    }
                }
            }
            return reader;
        }

        @Override
        public Fields get(int doc) throws IOException {
            return force().get(doc);
        }

        @Override
        public void checkIntegrity() {
            throw new UnsupportedOperationException();
        }

        @Override
        public TermVectorsReader clone() {
            return new LazyTermVectorsReader(() -> force().clone());
        }

        @Override
        public void close() throws IOException {
            final TermVectorsReader reader = this.reader;
            if (reader != null) {
                reader.close();
            }
        }

        @Override
        public long ramBytesUsed() {
            final TermVectorsReader reader = this.reader;
            if (reader == null) {
                return 0L;
            } else {
                return reader.ramBytesUsed();
            }
        }
    }

    private static class LazyPointsFormat extends PointsFormat {

        final PointsFormat delegate;

        LazyPointsFormat(PointsFormat delegate) {
            this.delegate = delegate;
        }

        @Override
        public PointsWriter fieldsWriter(SegmentWriteState state) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PointsReader fieldsReader(SegmentReadState state) {
            return new LazyPointsReader(() -> delegate.fieldsReader(state));
        }
    }

    private static class LazyPointsReader extends PointsReader {

        private final CheckedSupplier<PointsReader, IOException> pointsReaderSupplier;
        private final Object mutex = new Object();
        private volatile PointsReader reader = null;

        LazyPointsReader(CheckedSupplier<PointsReader, IOException> pointsReaderSupplier) {
            this.pointsReaderSupplier = pointsReaderSupplier;
        }

        PointsReader force() throws IOException {
            if (reader == null) {
                synchronized (mutex) {
                    if (reader == null) {
                        reader = pointsReaderSupplier.get();
                    }
                }
            }
            return reader;
        }

        @Override
        public void checkIntegrity() {
            throw new UnsupportedOperationException();
        }

        @Override
        public PointValues getValues(String field) throws IOException {
            return force().getValues(field);
        }

        @Override
        public void close() throws IOException {
            final PointsReader reader = this.reader;
            if (reader != null) {
                reader.close();
            }
        }

        @Override
        public long ramBytesUsed() {
            final PointsReader reader = this.reader;
            if (reader == null) {
                return 0L;
            } else {
                return reader.ramBytesUsed();
            }
        }
    }
}
