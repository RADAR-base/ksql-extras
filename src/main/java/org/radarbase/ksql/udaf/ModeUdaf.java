package org.radarbase.ksql.udaf;

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import java.util.List;
import org.apache.commons.math3.stat.StatUtils;
import org.apache.kafka.connect.data.Schema;
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
            aggregateSchema = "STRUCT<SAMPLES ARRAY<double>, COUNT bigint>"
    )
    public static Udaf<Double, Struct, Double> createUdaf() {
        return new ModeUdafImpl();
    }

    private static class ModeUdafImpl extends UniformSamplingReservoirUdaf<Double> {

        public ModeUdafImpl() {
            super(Schema.OPTIONAL_FLOAT64_SCHEMA);
        }

        @Override
        public Double map(Struct agg) {
            List<Double> samples = agg.getArray(UniformSamplingReservoirUdaf.SAMPLES);
            if (samples.isEmpty()) return null;

            return StatUtils.mode(samples.stream().mapToDouble(v -> v).toArray())[0];
        }
    }
}
