package org.radarbase.ksql.udaf;

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import java.util.List;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.kafka.connect.data.Struct;

@UdafDescription(name = "iqr",
        author = "yatharthranjan",
        version = "1.0.0",
        description = "Calculates the Inter-Quartile Range of the values in a stream.")
public class InterQuartileRangeUdaf {

    private InterQuartileRangeUdaf() {

    }

    @UdafFactory(
            description = "Calculates the Inter-Quartile Range of values in a stream.",
            aggregateSchema = "STRUCT<SAMPLES ARRAY<double>, COUNT bigint>"
    )
    public static Udaf<Double, Struct, Double> createUdaf() {
        return new InterQuartileRangeUdafImpl();
    }

    private static class InterQuartileRangeUdafImpl extends UniformSamplingReservoirDoubleUdaf {

        @Override
        public Double map(Struct agg) {
            List<Double> samples = agg.getArray(UniformSamplingReservoirDoubleUdaf.SAMPLES);
            if (samples.isEmpty()) return null;

            DescriptiveStatistics ds =
                    new DescriptiveStatistics(samples.stream().mapToDouble(v -> v).toArray());
            return (ds.getPercentile(75) - ds.getPercentile(25));
        }
    }
}
