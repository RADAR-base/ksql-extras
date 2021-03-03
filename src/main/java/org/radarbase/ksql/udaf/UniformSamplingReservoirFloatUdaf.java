package org.radarbase.ksql.udaf;

import io.confluent.ksql.function.udaf.Udaf;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

public abstract class UniformSamplingReservoirFloatUdaf implements Udaf<Float, Struct, Float> {

    public static final String VALUES = "VALUES";
    public static final String COUNT = "COUNT";
    public static final Schema STRUCT_RESERVOIR = SchemaBuilder.struct().optional()
            .field(VALUES, SchemaBuilder.array(Schema.FLOAT32_SCHEMA).optional())
            .field(COUNT, Schema.OPTIONAL_INT64_SCHEMA)
            .build();
    private static final int MAX_SIZE_DEFAULT = 9999;
    private final int maxSize;

    public UniformSamplingReservoirFloatUdaf() {
        this.maxSize = MAX_SIZE_DEFAULT;
    }

    public UniformSamplingReservoirFloatUdaf(int maxSize) {
        this.maxSize = maxSize;
    }

    @Override
    public Struct initialize() {
        return new Struct(STRUCT_RESERVOIR)
                .put(VALUES, new ArrayList<Float>() {
                })
                .put(COUNT, 0);
    }

    @Override
    public Struct aggregate(Float current, Struct aggregate) {
        if (current==null) return aggregate;

        List<Float> samples = aggregate.getArray(VALUES);
        long count = aggregate.getInt64(COUNT);

        return new Struct(STRUCT_RESERVOIR)
                .put(VALUES, add(current, samples, count))
                .put(COUNT, ++count);
    }

    private List<Float> add(Float current, List<Float> samples, long count) {
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