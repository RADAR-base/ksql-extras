package org.radarbase.ksql.udaf;

import io.confluent.ksql.function.udaf.Udaf;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

public abstract class UniformSamplingReservoirDoubleUdaf implements Udaf<Double, Struct, Double> {

    public static final String SAMPLES = "SAMPLES";
    public static final String COUNT = "COUNT";
    public static final Schema STRUCT_RESERVOIR = SchemaBuilder.struct().optional()
            .field(SAMPLES, SchemaBuilder.array(Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build())
            .field(COUNT, Schema.OPTIONAL_INT64_SCHEMA)
            .build();
    private static final int MAX_SIZE_DEFAULT = 9999;
    private final int maxSize;

    public UniformSamplingReservoirDoubleUdaf() {
        this.maxSize = MAX_SIZE_DEFAULT;
    }

    public UniformSamplingReservoirDoubleUdaf(int maxSize) {
        this.maxSize = maxSize;
    }

    @Override
    public Struct initialize() {
        return new Struct(STRUCT_RESERVOIR)
                .put(SAMPLES, new ArrayList<Double>() {
                })
                .put(COUNT, 0L);
    }

    @Override
    public Struct aggregate(Double current, Struct aggregate) {
        if (current==null) return aggregate;

        List<Double> samples = aggregate.getArray(SAMPLES);
        long count = aggregate.getInt64(COUNT);

        return new Struct(STRUCT_RESERVOIR)
                .put(SAMPLES, add(current, samples, count))
                .put(COUNT, ++count);
    }

    private List<Double> add(Double current, List<Double> samples, long count) {
        if (samples.size()==maxSize) {
            long replaceIndex = ThreadLocalRandom.current().nextLong(count);
            if (replaceIndex < maxSize) {
                samples.set((int) replaceIndex, current);
            }
        } else {
            samples.add(current);
        }
        return samples;
    }

    @Override
    public Struct merge(Struct aggOne, Struct aggTwo) {
        return aggTwo;
    }

    public int getMaxSize() {
        return maxSize;
    }
}