package org.radarbase.ksql.udaf;

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import java.util.List;
import org.apache.commons.math3.stat.StatUtils;
import org.apache.kafka.connect.data.Struct;

@UdafDescription(name = "mode",
        author = "yatharthranjan",
        version = "1.0.0",
        description = "Calculates the mode of numbers in a stream.")
public class ModeUdaf {

    private ModeUdaf() {

    }

    @UdafFactory(
            description = "Calculates the mode of values in a stream.",
            aggregateSchema = "STRUCT<VALUES ARRAY<float>, COUNT bigint>"
    )
    public static Udaf<Float, Struct, Float> createUdaf() {
        return new ModeUdafImpl();
    }

    private static class ModeUdafImpl extends UniformSamplingReservoirFloatUdaf {

        @Override
        public Float map(Struct agg) {
            List<Float> samples = agg.getArray(UniformSamplingReservoirFloatUdaf.VALUES);
            if (samples.isEmpty()) return 0f;

            return (float) StatUtils.mode(samples.stream().mapToDouble(v -> v).toArray())[0];
        }
    }
}
