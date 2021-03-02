package org.radarbase.ksql.udaf;


import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.math3.stat.descriptive.moment.Skewness;

@UdafDescription(name = "skew",
        author = "yatharthranjan",
        version = "1.0.0",
        description = "Calculates the skewness of the distribution in a stream.")
public class SkewnessUdaf {

    private SkewnessUdaf() {

    }

    @UdafFactory(description = "Calculates the skewness of values in a stream.")
    public static Udaf<Float, List<Float>, Float> createUdaf() {
        return new SkewnessUdafImpl();
    }

    private static class SkewnessUdafImpl implements Udaf<Float, List<Float>, Float> {

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

            return (float) new Skewness().evaluate(agg.stream().mapToDouble(v -> v).toArray());
        }
    }

}
