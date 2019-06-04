package com.github.flinkbwa;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Function to combine 2 Dataset<Tuple2<Long, String>>.
 * @return A Tuple2<Long, Tuple2<String, String>> which contains the common key of the datasets and the strings of the both.
 *
 * Created by silvia on 23/04/19.
 */
public class FASTQPairMapOperator implements MapFunction<Tuple2<Tuple2<Long, String>, Tuple2<Long, String>>, Tuple2<Long, Tuple2<String, String>>> {

    public Tuple2<Long, Tuple2<String, String>> map(Tuple2<Tuple2<Long, String>, Tuple2<Long, String>> tuples) throws Exception {
        return Tuple2.of(tuples.f0.f0, Tuple2.of(tuples.f0.f1, tuples.f1.f1));
    }
}
