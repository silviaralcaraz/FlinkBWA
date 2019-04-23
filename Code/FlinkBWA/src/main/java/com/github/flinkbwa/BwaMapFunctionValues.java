package com.github.flinkbwa;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Created by silvia on 20/04/19.
 */

public class BwaMapFunctionValues implements MapFunction<Tuple2<Long, String>, String> {

    public String map(Tuple2<Long, String> longStringTuple2) throws Exception {
        return longStringTuple2.f1;
    }
}
