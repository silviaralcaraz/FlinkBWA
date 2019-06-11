package com.github.flinkbwa;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Class to get the key of a DataSet<Tuple2<Long, Tuple2<String, String>>
 *
 * Created by silvia on 06/06/19.
 */
public class FASTQKeySelectorPaired implements KeySelector<Tuple2<Long, Tuple2<String, String>>, Long> {

    @Override
    public Long getKey(Tuple2<Long, Tuple2<String, String>> input) throws Exception {
        return input.f0;
    }
}
