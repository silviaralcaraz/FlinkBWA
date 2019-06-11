package com.github.flinkbwa;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Class that implements functionality of grouping FASTQ without indexed PairDataset
 *
 * Created by silvia on 10/04/19.
 */
public class FASTQRecordCreator implements GroupReduceFunction<Tuple2<Long, Tuple2<Long, String>>, Tuple2<Long, String>> {

    public void reduce(Iterable<Tuple2<Long, Tuple2<Long, String>>> iterable, Collector<Tuple2<Long, String>> collector) throws Exception {
        // We create the data to be contained inside the record
        String seqName = null;
        String seq = null;
        String qual = null;
        String extraSeqname = null;
        Long id = 0L;

        // Keep in mind that records are sorted by key. This is, we have 4 available lines here
        for (Tuple2<Long, Tuple2<Long, String>> item : iterable) {
            id = item.f0;
            Tuple2<Long, String> record = item.f1;
            Long lineNum = record.f0;
            String line = record.f1;

            if (lineNum == 0) {
                seqName = line;
            }
            else if (lineNum == 1) {
                seq = line;
            }
            else if (lineNum == 2) {
                extraSeqname = line;
            }
            else {
                qual = line;
            }
        }

        // If everything went fine, we return the current record
        if (seqName != null && seq != null && qual != null && extraSeqname != null) {
            collector.collect(new Tuple2<Long, String>(id, String.format("%s\n%s\n%s\n%s\n", seqName, seq, extraSeqname, qual)));
        } else {
            System.err.println("Malformed record!");
            System.err.println(String.format("%d - %s\n%s\n%s\n%s\n", id, seqName, seq, extraSeqname, qual));
        }
    }
}
