package com.github.flinkbwa;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Class to return the string field of given Tuple2.
 * @input Dataset<Tuple2<Long,String>>
 * @result String (second field of Tuple2)
 * @throws Exception
 *
 * Created by silvia on 20/04/19.
 */

public class BwaMapFunctionValues implements MapFunction<Tuple2<Long, String>, String> {

    public String map(Tuple2<Long, String> input) throws Exception {
        return input.f1;
    }
}
