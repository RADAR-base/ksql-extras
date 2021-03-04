package org.radarbase.ksql.udaf;

import io.confluent.ksql.function.udaf.Udaf;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

/**
 * Uses uniform sampling reservoir to add new data point to the aggregate using Algorithm-R.
 * <href>https://en.wikipedia.org/wiki/Reservoir_sampling</href>
 *
 * <p>The maximum size of the reservoir can be increased to get more accurate estimations.
 * As long as the number of samples is lower than the maximum size of the reservoir, the measures
 * are computed exactly.
 */
@SuppressWarnings("PMD.DataflowAnomalyAnalysis")
public abstract class UniformSamplingReservoirUdaf<T> implements Udaf<T, Struct, T> {

    public static final String SAMPLES = "SAMPLES";
    public static final String COUNT = "COUNT";
    private static final int MAX_SIZE_DEFAULT = 1000;
    private final Schema structSchema;
    private final int maxSize;

    public UniformSamplingReservoirUdaf(Schema arrayValues) {
        this.maxSize = MAX_SIZE_DEFAULT;
        structSchema = SchemaBuilder.struct().optional()
                .field(SAMPLES, SchemaBuilder.array(arrayValues).optional().build())
                .field(COUNT, Schema.OPTIONAL_INT64_SCHEMA)
                .build();
    }

    public UniformSamplingReservoirUdaf(int maxSize, Schema arrayValues) {
        this.maxSize = maxSize;
        structSchema = SchemaBuilder.struct().optional()
                .field(SAMPLES, SchemaBuilder.array(arrayValues).optional().build())
                .field(COUNT, Schema.OPTIONAL_INT64_SCHEMA)
                .build();
    }

    @Override
    public Struct initialize() {
        return new Struct(structSchema)
                .put(SAMPLES, new ArrayList<T>() {
                })
                .put(COUNT, 0L);
    }

    @Override
    public Struct aggregate(T current, Struct aggregate) {
        if (current==null) return aggregate;

        List<T> samples = aggregate.getArray(SAMPLES);
        long count = aggregate.getInt64(COUNT);

        return new Struct(structSchema)
                .put(SAMPLES, add(current, samples, count))
                .put(COUNT, ++count);
    }

    private List<T> add(T current, List<T> samples, long count) {
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

    /**
     * Merges the aggregates by randomly selecting data points from the two aggregates based on
     * probability calculated by size of each aggregate.
     *
     * @param aggOne aggregate one
     * @param aggTwo aggregate two
     * @return merged result of the two aggregates
     */
    @Override
    public Struct merge(Struct aggOne, Struct aggTwo) {
        List<T> samples1 = aggOne.getArray(SAMPLES);
        List<T> samples2 = aggTwo.getArray(SAMPLES);

        if (samples1.isEmpty()) return aggTwo;
        if (samples2.isEmpty()) return aggOne;

        List<T> newSamples = new ArrayList<>();
        Random random = ThreadLocalRandom.current();
        long aggOneCount = aggOne.getInt64(COUNT);
        long aggTwoCount = aggTwo.getInt64(COUNT);
        double prob1 = aggOneCount / (double) (aggOneCount + aggTwoCount);

        while (newSamples.size() < maxSize && (!samples1.isEmpty() || !samples2.isEmpty())) {
            List<T> sampleList;
            if (samples2.isEmpty()
                    || (!samples1.isEmpty()
                    && random.nextDouble() < prob1)
            ) {
                sampleList = samples1;
            } else {
                sampleList = samples2;
            }
            int idx = random.nextInt(sampleList.size());
            newSamples.add(sampleList.remove(idx));
        }

        return new Struct(structSchema)
                .put(SAMPLES, newSamples)
                .put(COUNT, aggOneCount + aggTwoCount);
    }

    public int getMaxSize() {
        return maxSize;
    }

    public Schema getStructSchema() {
        return structSchema;
    }
}