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
            description = "Calculates the standard deviation of float values in a stream.",
            aggregateSchema = "STRUCT<VALUES ARRAY<float>, COUNT bigint>"
    )
    public static Udaf<Float, Struct, Float> createUdaf() {
        return new StdDevUdafImpl();
    }

    private static class StdDevUdafImpl extends UniformSamplingReservoirFloatUdaf {

        @Override
        public Float map(Struct agg) {
            List<Float> samples = agg.getArray(UniformSamplingReservoirFloatUdaf.VALUES);
            if (samples.isEmpty()) return 0f;

            double stdDev =
                    Math.sqrt(StatUtils.variance(samples.stream().mapToDouble(v -> v).toArray()));

            return (float) stdDev;
        }
    }
}
