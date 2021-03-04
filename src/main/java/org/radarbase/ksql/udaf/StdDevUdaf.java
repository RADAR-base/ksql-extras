package org.radarbase.ksql.udaf;

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import java.util.List;
import org.apache.commons.math3.stat.StatUtils;
import org.apache.kafka.connect.data.Struct;

@UdafDescription(name = "std_dev",
        author = "yatharthranjan",
        version = "1.0.0",
        description = "Calculates the standard deviation of numbers in a stream.")
public class StdDevUdaf {

    private StdDevUdaf() {
    }

    @UdafFactory(
            description = "Calculates the standard deviation of double values in a stream.",
            aggregateSchema = "STRUCT<SAMPLES ARRAY<double>, COUNT bigint>"
    )
    public static Udaf<Double, Struct, Double> createUdaf() {
        return new StdDevUdafImpl();
    }

    private static class StdDevUdafImpl extends UniformSamplingReservoirDoubleUdaf {

        public StdDevUdafImpl() {
            super(5000);
        }

        @Override
        public Double map(Struct agg) {
            List<Double> samples = agg.getArray(UniformSamplingReservoirDoubleUdaf.SAMPLES);
            if (samples.isEmpty()) return null;

            return Math.sqrt(StatUtils.variance(samples.stream().mapToDouble(v -> v).toArray()));
        }
    }
}
