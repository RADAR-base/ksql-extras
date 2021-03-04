package org.radarbase.ksql.udaf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.radarbase.ksql.udaf.UniformSamplingReservoirUdaf.COUNT;
import static org.radarbase.ksql.udaf.UniformSamplingReservoirUdaf.SAMPLES;

import io.confluent.ksql.function.udaf.Udaf;
import java.util.ArrayList;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class UdafTest {

    private static final Double[] values = new Double[]{40.0, 50.0, 60.0};
    private final transient Schema structSchema = SchemaBuilder.struct().optional()
            .field(SAMPLES, SchemaBuilder.array(Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build())
            .field(COUNT, Schema.OPTIONAL_INT64_SCHEMA)
            .build();
    private transient Struct struct;

    @BeforeEach
    void setupEach() {
        struct = new Struct(structSchema)
                .put(SAMPLES, new ArrayList<Double>())
                .put(COUNT, 0L);
    }

    @Test
    void calculateStdDev() {
        Udaf<Double, Struct, Double> stdDevUdaf = StdDevUdaf.createUdaf();

        for (Double currrent : values) {
            struct = stdDevUdaf.aggregate(currrent, struct);
        }

        assertEquals(10.0, stdDevUdaf.map(struct));
    }

    @Test
    void calculateModeTest() {
        Udaf<Double, Struct, Double> modeUdaf = ModeUdaf.createUdaf();

        for (Double currrent : values) {
            struct = modeUdaf.aggregate(currrent, struct);
        }
        // Add an extra value for higher frequency
        struct = modeUdaf.aggregate(50.0, struct);

        assertEquals(50.0, modeUdaf.map(struct));
    }

    @Test
    void calculateMedianTest() {
        Udaf<Double, Struct, Double> medianUdaf = MedianUdaf.createUdaf();

        for (Double currrent : values) {
            struct = medianUdaf.aggregate(currrent, struct);
        }

        assertEquals(50.0, medianUdaf.map(struct));

        // add value to check the case when n is even
        struct = medianUdaf.aggregate(70.0, struct);

        assertEquals(55.0, medianUdaf.map(struct));
    }

    @Test
    void calculateSkewTest() {
        Udaf<Double, Struct, Double> skewUdaf = SkewnessUdaf.createUdaf();

        for (Double currrent : values) {
            struct = skewUdaf.aggregate(currrent, struct);
        }

        assertEquals(0.0, skewUdaf.map(struct));

        // Add a value to create some skew
        struct = skewUdaf.aggregate(90.0, struct);

        assertNotEquals(0.0, skewUdaf.map(struct));
    }

    @Test
    void calculateIqrTest() {
        Udaf<Double, Struct, Double> iqrUdaf = InterQuartileRangeUdaf.createUdaf();

        for (Double currrent : values) {
            struct = iqrUdaf.aggregate(currrent, struct);
        }

        struct = iqrUdaf.aggregate(70.0, struct);

        assertEquals(25.0, iqrUdaf.map(struct));
    }
}
