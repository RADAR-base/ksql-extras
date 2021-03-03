package org.radarbase.ksql.udaf;

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import java.util.List;
import org.apache.commons.math3.stat.StatUtils;

@UdafDescription(name = "mode",
        author = "yatharthranjan",
        version = "1.0.0",
        description = "Calculates the mode of numbers in a stream.")
public class ModeUdaf {

    private ModeUdaf() {

    }

    @UdafFactory(description = "Calculates the mode of values in a stream.")
    public static Udaf<Float, List<Float>, Float> createUdaf() {
        return new ModeUdafImpl();
    }

    private static class ModeUdafImpl extends AbstractListUdaf<Float> {

        @Override
        public Float map(List<Float> agg) {
            if (agg.isEmpty()) return 0f;

            return (float) StatUtils.mode(agg.stream().mapToDouble(v -> v).toArray())[0];
        }
    }
}
