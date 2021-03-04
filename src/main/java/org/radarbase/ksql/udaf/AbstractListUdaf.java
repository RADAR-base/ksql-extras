package org.radarbase.ksql.udaf;

import io.confluent.ksql.function.udaf.Udaf;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractListUdaf<T> implements Udaf<T, List<T>, T> {

    @Override
    public List<T> initialize() {
        return new ArrayList<>();
    }

    @Override
    public List<T> aggregate(T current, List<T> aggregate) {
        if (current!=null) aggregate.add(current);
        return aggregate;
    }

    @Override
    public List<T> merge(List<T> aggOne, List<T> aggTwo) {
        aggOne.addAll(aggTwo);
        return aggOne;
    }
}