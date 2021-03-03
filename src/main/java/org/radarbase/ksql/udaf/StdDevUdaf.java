package org.radarbase.ksql.udaf;

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import java.util.List;
import org.apache.commons.math3.stat.StatUtils;

@UdafDescription(name = "std_dev",
        author = "yatharthranjan",
        version = "1.0.0",
        description = "Calculates the standard deviation of numbers in a stream.")
public class StdDevUdaf {

    private StdDevUdaf() {
    }

    @UdafFactory(description = "Calculates the standard deviation of float values in a stream.")
    public static Udaf<Float, List<Float>, Float> createUdaf() {
        return new StdDevUdafImpl();
    }

    private static class StdDevUdafImpl extends AbstractListUdaf<Float> {

        @Override
        public Float map(List<Float> agg) {
            if (agg.isEmpty()) return 0f;

            double stdDev =
                    Math.sqrt(StatUtils.variance(agg.stream().mapToDouble(v -> v).toArray()));

            return (float) stdDev;
        }
    }
}
