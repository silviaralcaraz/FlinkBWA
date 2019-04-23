package com.github.flinkbwa;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Created by silvia on 23/04/19.
 */
public class FASTQPairMapOperator implements org.apache.flink.api.common.functions.MapFunction<org.apache.flink.api.java.tuple.Tuple2<org.apache.flink.api.java.tuple.Tuple2<Long, String>, org.apache.flink.api.java.tuple.Tuple2<Long, String>>, org.apache.flink.api.java.tuple.Tuple2<Long, org.apache.flink.api.java.tuple.Tuple2<String, String>>> {
    public Tuple2<Long, Tuple2<String, String>> map(Tuple2<Tuple2<Long, String>, Tuple2<Long, String>> tuple2Tuple2Tuple2) throws Exception {
        return Tuple2.of(tuple2Tuple2Tuple2.f0.f0, Tuple2.of(tuple2Tuple2Tuple2.f0.f1, tuple2Tuple2Tuple2.f1.f1));
    }
}
