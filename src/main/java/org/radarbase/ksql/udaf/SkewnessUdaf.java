package org.radarbase.ksql.udaf;


import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import java.util.List;
import org.apache.commons.math3.stat.descriptive.moment.Skewness;
import org.apache.kafka.connect.data.Struct;

@UdafDescription(name = "skew",
        author = "yatharthranjan",
        version = "1.0.0",
        description = "Calculates the skewness of the distribution in a stream.")
public class SkewnessUdaf {

    private SkewnessUdaf() {

    }

    @UdafFactory(
            description = "Calculates the skewness of values in a stream.",
            aggregateSchema = "STRUCT<SAMPLES ARRAY<double>, COUNT bigint>"
    )
    public static Udaf<Double, Struct, Double> createUdaf() {
        return new SkewnessUdafImpl();
    }

    private static class SkewnessUdafImpl extends UniformSamplingReservoirDoubleUdaf {

        public SkewnessUdafImpl() {
            super(5000);
        }

        @Override
        public Double map(Struct agg) {
            List<Double> samples = agg.getArray(UniformSamplingReservoirDoubleUdaf.SAMPLES);
            if (samples.isEmpty()) return null;

            return new Skewness().evaluate(samples.stream().mapToDouble(v -> v).toArray());
        }
    }

}
