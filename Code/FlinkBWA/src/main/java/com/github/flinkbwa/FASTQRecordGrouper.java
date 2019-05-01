/**
 * Copyright 2016 José Manuel Abuín Mosquera <josemanuel.abuin@usc.es>
 *
 * <p>This file is part of SparkBWA.
 *
 * <p>SparkBWA is free software: you can redistribute it and/or modify it under the terms of the GNU
 * General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * <p>SparkBWA is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * <p>You should have received a copy of the GNU General Public License along with SparkBWA. If not,
 * see <http://www.gnu.org/licenses/>.
 */

package com.github.flinkbwa;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Class that implements functionality of indexing FASTQ input lines into groups of 4 (one read)
 */

public class FASTQRecordGrouper implements MapFunction<Tuple2<Long, String>, Tuple2<Long, Tuple2<Long, String>>> {

    public Tuple2<Long, Tuple2<Long, String>> map(Tuple2<Long, String> longStringTuple2) throws Exception {
        // We get string input and line number
        String line = longStringTuple2.f1;
        Long fastqLineNum = longStringTuple2.f0;

        // We obtain the record number from the line number
        Long recordNum = (long) Math.floor(fastqLineNum / 4);
        // We form the pair <String line, Long index inside record (0..3)>
        Tuple2<Long, String> newRecordTuple = new Tuple2<Long, String>(fastqLineNum%4, line);

        return new Tuple2<Long,Tuple2<Long, String>>(recordNum, newRecordTuple);
    }
}
