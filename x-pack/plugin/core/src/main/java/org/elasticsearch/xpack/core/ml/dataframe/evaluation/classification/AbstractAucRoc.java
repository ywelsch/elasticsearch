/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.metrics.Percentiles;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationMetric;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationMetricResult;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.MlEvaluationNamedXContentProvider.registeredMetricName;

/**
 * Area under the curve (AUC) of the receiver operating characteristic (ROC).
 * The ROC curve is a plot of the TPR (true positive rate) against
 * the FPR (false positive rate) over a varying threshold.
 *
 * This particular implementation is making use of ES aggregations
 * to calculate the curve. It then uses the trapezoidal rule to calculate
 * the AUC.
 *
 * In particular, in order to calculate the ROC, we get percentiles of TP
 * and FP against the predicted probability. We call those Rate-Threshold
 * curves. We then scan ROC points from each Rate-Threshold curve against the
 * other using interpolation. This gives us an approximation of the ROC curve
 * that has the advantage of being efficient and resilient to some edge cases.
 *
 * When this is used for multi-class classification, it will calculate the ROC
 * curve of each class versus the rest.
 */
public abstract class AbstractAucRoc implements EvaluationMetric {

    public static final ParseField NAME = new ParseField("auc_roc");

    protected AbstractAucRoc() {}

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }

    protected static double[] percentilesArray(Percentiles percentiles) {
        double[] result = new double[99];
        percentiles.forEach(percentile -> {
            if (Double.isNaN(percentile.getValue())) {
                throw ExceptionsHelper.badRequestException(
                    "[{}] requires at all the percentiles values to be finite numbers", NAME.getPreferredName());
            }
            result[((int) percentile.getPercent()) - 1] = percentile.getValue();
        });
        return result;
    }

    /**
     * Visible for testing
     */
    protected static List<AucRocPoint> buildAucRocCurve(double[] tpPercentiles, double[] fpPercentiles) {
        assert tpPercentiles.length == fpPercentiles.length;
        assert tpPercentiles.length == 99;

        List<AucRocPoint> aucRocCurve = new ArrayList<>();
        aucRocCurve.add(new AucRocPoint(0.0, 0.0, 1.0));
        aucRocCurve.add(new AucRocPoint(1.0, 1.0, 0.0));
        RateThresholdCurve tpCurve = new RateThresholdCurve(tpPercentiles, true);
        RateThresholdCurve fpCurve = new RateThresholdCurve(fpPercentiles, false);
        aucRocCurve.addAll(tpCurve.scanPoints(fpCurve));
        aucRocCurve.addAll(fpCurve.scanPoints(tpCurve));
        Collections.sort(aucRocCurve);
        return aucRocCurve;
    }

    /**
     * Visible for testing
     */
    protected static double calculateAucScore(List<AucRocPoint> rocCurve) {
        // Calculates AUC based on the trapezoid rule
        double aucRoc = 0.0;
        for (int i = 1; i < rocCurve.size(); i++) {
            AucRocPoint left = rocCurve.get(i - 1);
            AucRocPoint right = rocCurve.get(i);
            aucRoc += (right.fpr - left.fpr) * (right.tpr + left.tpr) / 2;
        }
        return aucRoc;
    }

    private static class RateThresholdCurve {

        private final double[] percentiles;
        private final boolean isTp;

        private RateThresholdCurve(double[] percentiles, boolean isTp) {
            this.percentiles = percentiles;
            this.isTp = isTp;
        }

        private double getRate(int index) {
            return 1 - 0.01 * (index + 1);
        }

        private double getThreshold(int index) {
            return percentiles[index];
        }

        private double interpolateRate(double threshold) {
            int binarySearchResult = Arrays.binarySearch(percentiles, threshold);
            if (binarySearchResult >= 0) {
                return getRate(binarySearchResult);
            } else {
                int right = (binarySearchResult * -1) -1;
                int left = right - 1;
                if (right >= percentiles.length) {
                    return 0.0;
                } else if (left < 0) {
                    return 1.0;
                } else {
                    double rightRate = getRate(right);
                    double leftRate = getRate(left);
                    return interpolate(threshold, percentiles[left], leftRate, percentiles[right], rightRate);
                }
            }
        }

        private List<AucRocPoint> scanPoints(RateThresholdCurve againstCurve) {
            List<AucRocPoint> points = new ArrayList<>();
            for (int index = 0; index < percentiles.length; index++) {
                double rate = getRate(index);
                double scannedThreshold = getThreshold(index);
                double againstRate = againstCurve.interpolateRate(scannedThreshold);
                AucRocPoint point;
                if (isTp) {
                    point = new AucRocPoint(rate, againstRate, scannedThreshold);
                } else {
                    point = new AucRocPoint(againstRate, rate, scannedThreshold);
                }
                points.add(point);
            }
            return points;
        }
    }

    public static final class AucRocPoint implements Comparable<AucRocPoint>, ToXContentObject, Writeable {

        private static final String TPR = "tpr";
        private static final String FPR = "fpr";
        private static final String THRESHOLD = "threshold";

        private final double tpr;
        private final double fpr;
        private final double threshold;

        AucRocPoint(double tpr, double fpr, double threshold) {
            this.tpr = tpr;
            this.fpr = fpr;
            this.threshold = threshold;
        }

        private AucRocPoint(StreamInput in) throws IOException {
            this.tpr = in.readDouble();
            this.fpr = in.readDouble();
            this.threshold = in.readDouble();
        }

        @Override
        public int compareTo(AucRocPoint o) {
            return Comparator.comparingDouble((AucRocPoint p) -> p.threshold).reversed()
                .thenComparing(p -> p.fpr)
                .thenComparing(p -> p.tpr)
                .compare(this, o);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeDouble(tpr);
            out.writeDouble(fpr);
            out.writeDouble(threshold);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(TPR, tpr);
            builder.field(FPR, fpr);
            builder.field(THRESHOLD, threshold);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AucRocPoint that = (AucRocPoint) o;
            return tpr == that.tpr
                && fpr == that.fpr
                && threshold == that.threshold;
        }

        @Override
        public int hashCode() {
            return Objects.hash(tpr, fpr, threshold);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }

    private static double interpolate(double x, double x1, double y1, double x2, double y2) {
        return y1 + (x - x1) * (y2 - y1) / (x2 - x1);
    }

    public static class Result implements EvaluationMetricResult {

        private static final String SCORE = "score";
        private static final String DOC_COUNT = "doc_count";
        private static final String CURVE = "curve";

        private final double score;
        private final Long docCount;
        private final List<AucRocPoint> curve;

        public Result(double score, Long docCount, List<AucRocPoint> curve) {
            this.score = score;
            this.docCount = docCount;
            this.curve = Objects.requireNonNull(curve);
        }

        public Result(StreamInput in) throws IOException {
            this.score = in.readDouble();
            this.docCount = in.readOptionalLong();
            this.curve = in.readList(AucRocPoint::new);
        }

        public double getScore() {
            return score;
        }

        public Long getDocCount() {
            return docCount;
        }

        public List<AucRocPoint> getCurve() {
            return Collections.unmodifiableList(curve);
        }

        @Override
        public String getWriteableName() {
            return registeredMetricName(Classification.NAME, NAME);
        }

        @Override
        public String getMetricName() {
            return NAME.getPreferredName();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeDouble(score);
            out.writeOptionalLong(docCount);
            out.writeList(curve);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(SCORE, score);
            if (docCount != null) {
                builder.field(DOC_COUNT, docCount);
            }
            if (curve.isEmpty() == false) {
                builder.field(CURVE, curve);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Result that = (Result) o;
            return score == that.score
                && Objects.equals(docCount, that.docCount)
                && Objects.equals(curve, that.curve);
        }

        @Override
        public int hashCode() {
            return Objects.hash(score, docCount, curve);
        }
    }
}
