package org.radarbase.ksql.udaf;

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import java.util.List;
import org.apache.commons.math3.stat.StatUtils;
import org.apache.kafka.connect.data.Struct;

@UdafDescription(name = "median",
        author = "yatharthranjan",
        version = "1.0.0",
        description = "Calculates the median of numbers in a stream.")
public class MedianUdaf {

    private MedianUdaf() {

    }

    @UdafFactory(
            description = "Calculates the median of values in a stream.",
            aggregateSchema = "STRUCT<VALUES ARRAY<float>, COUNT bigint>"
    )
    public static Udaf<Float, Struct, Float> createUdaf() {
        return new MedianUdafImpl();
    }

    private static class MedianUdafImpl extends UniformSamplingReservoirFloatUdaf {

        @Override
        public Float map(Struct agg) {
            List<Float> samples = agg.getArray(UniformSamplingReservoirFloatUdaf.VALUES);
            if (samples.isEmpty()) return 0f;

            return (float) StatUtils.percentile(samples.stream().mapToDouble(v -> v).toArray(), 50);
        }
    }
}
