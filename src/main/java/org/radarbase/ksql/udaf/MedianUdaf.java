package org.radarbase.ksql.udaf;

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.math3.stat.StatUtils;

@UdafDescription(name = "median",
        author = "yatharthranjan",
        version = "1.0.0",
        description = "Calculates the median of numbers in a stream.")
public class MedianUdaf {

    private MedianUdaf() {

    }

    @UdafFactory(description = "Calculates the median of values in a stream.")
    public static Udaf<Float, List<Float>, Float> createUdaf() {
        return new MedianUdafImpl();
    }

    private static class MedianUdafImpl implements Udaf<Float, List<Float>, Float> {

        @Override
        public List<Float> initialize() {
            return new ArrayList<>();
        }

        @Override
        public List<Float> aggregate(Float current, List<Float> aggregate) {
            if (current!=null) aggregate.add(current);
            return aggregate;
        }

        @Override
        public List<Float> merge(List<Float> aggOne, List<Float> aggTwo) {
            return aggTwo;
        }

        @Override
        public Float map(List<Float> agg) {
            if (agg.isEmpty()) return 0f;

            return (float) StatUtils.percentile(agg.stream().mapToDouble(v -> v).toArray(), 50);
        }
    }
}
