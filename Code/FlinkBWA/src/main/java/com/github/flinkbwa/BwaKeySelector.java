package com.github.flinkbwa;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Created by silvia on 15/04/19.
 */
public class BwaKeySelector implements KeySelector<Tuple2<Long, String>, Long> {
    public Long getKey(Tuple2<Long, String> longStringTuple2) throws Exception {
        return longStringTuple2.f0;
    }
}
