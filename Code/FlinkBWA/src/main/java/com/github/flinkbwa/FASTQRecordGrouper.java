package com.github.flinkbwa;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Class that implements functionality of indexing FASTQ input lines into groups of 4 (one read)
 *
 * Created by silvia on 10/04/19.
 */

public class FASTQRecordGrouper implements MapFunction<Tuple2<Long, String>, Tuple2<Long, Tuple2<Long, String>>> {

    public Tuple2<Long, Tuple2<Long, String>> map(Tuple2<Long, String> input) throws Exception {
        // We get string input and line number
        String line = input.f1;
        Long fastqLineNum = input.f0;

        // We obtain the record number from the line number
        Long recordNum = (long) Math.floor(fastqLineNum / 4);
        // We form the pair <String line, Long index inside record (0..3)>
        Tuple2<Long, String> newRecordTuple = new Tuple2<Long, String>(fastqLineNum%4, line);

        return new Tuple2<Long,Tuple2<Long, String>>(recordNum, newRecordTuple);
    }
}
