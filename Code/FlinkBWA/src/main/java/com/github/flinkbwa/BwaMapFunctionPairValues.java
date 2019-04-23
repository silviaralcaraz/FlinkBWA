package com.github.flinkbwa;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Created by silvia on 15/04/19.
 */
public class BwaMapFunctionPairValues implements MapFunction<Tuple2<Long, Tuple2<String, String>>, Tuple2<String, String>> {

    public Tuple2<String, String> map(Tuple2<Long, Tuple2<String, String>> longTuple2Tuple2) throws Exception {
        return longTuple2Tuple2.f1;
    }
}
