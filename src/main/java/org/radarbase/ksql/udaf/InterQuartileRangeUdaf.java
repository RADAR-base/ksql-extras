package org.radarbase.ksql.udaf;

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import java.util.List;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

@UdafDescription(name = "iqr",
        author = "yatharthranjan",
        version = "1.0.0",
        description = "Calculates the Inter-Quartile Range of the values in a stream.")
public class InterQuartileRangeUdaf {

    private InterQuartileRangeUdaf() {

    }

    @UdafFactory(description = "Calculates the Inter-Quartile Range of values in a stream.")
    public static Udaf<Float, List<Float>, Float> createUdaf() {
        return new InterQuartileRangeUdafImpl();
    }

    private static class InterQuartileRangeUdafImpl extends AbstractListUdaf<Float> {

        @Override
        public Float map(List<Float> agg) {
            if (agg.isEmpty()) return 0f;

            DescriptiveStatistics ds =
                    new DescriptiveStatistics(agg.stream().mapToDouble(v -> v).toArray());
            return (float) (ds.getPercentile(75) - ds.getPercentile(25));
        }
    }
}
