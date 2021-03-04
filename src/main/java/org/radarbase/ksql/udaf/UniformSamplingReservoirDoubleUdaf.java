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
public abstract class UniformSamplingReservoirDoubleUdaf implements Udaf<Double, Struct, Double> {

    public static final String SAMPLES = "SAMPLES";
    public static final String COUNT = "COUNT";
    public static final Schema STRUCT_RESERVOIR = SchemaBuilder.struct().optional()
            .field(SAMPLES, SchemaBuilder.array(Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build())
            .field(COUNT, Schema.OPTIONAL_INT64_SCHEMA)
            .build();
    private static final int MAX_SIZE_DEFAULT = 1000;
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
        List<Double> samples1 = aggOne.getArray(SAMPLES);
        List<Double> samples2 = aggTwo.getArray(SAMPLES);

        if (samples1.isEmpty()) return aggTwo;
        if (samples2.isEmpty()) return aggOne;

        List<Double> newSamples = new ArrayList<>();
        Random random = ThreadLocalRandom.current();
        long aggOneCount = aggOne.getInt64(COUNT);
        long aggTwoCount = aggTwo.getInt64(COUNT);
        double prob1 = aggOneCount / (double) (aggOneCount + aggTwoCount);

        while (newSamples.size() < maxSize && (!samples1.isEmpty() || !samples2.isEmpty())) {
            List<Double> sampleList;
            if (samples2.isEmpty()
                    || (!samples1.isEmpty()
                    && ThreadLocalRandom.current().nextDouble() < prob1)
            ) {
                sampleList = samples1;
            } else {
                sampleList = samples2;
            }
            int idx = ThreadLocalRandom.current().nextInt(sampleList.size());
            newSamples.add(sampleList.remove(idx));
        }

        return new Struct(STRUCT_RESERVOIR)
                .put(SAMPLES, newSamples)
                .put(COUNT, aggOneCount + aggTwoCount);
    }

    public int getMaxSize() {
        return maxSize;
    }
}