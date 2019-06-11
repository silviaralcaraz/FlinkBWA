package com.github.flinkbwa;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Class to get the key of a DataSet<Tuple2<Long, String>>
 *
 * Created by silvia on 15/04/19.
 */
public class FASTQKeySelector implements KeySelector<Tuple2<Long, String>, Long> {

    public Long getKey(Tuple2<Long, String> Tuple2) throws Exception {
        return Tuple2.f0;
    }
}
