package com.github.flinkbwa;

//import org.apache.spark.api.java.function.Function;
import org.apache.flink.curator.shaded.com.google.common.base.Function;
import org.apache.flink.api.java.tuple.Tuple2;
import javax.annotation.Nullable;

/**
 * Class that implements functionality of grouping FASTQ without indexed PairRDD -> In this case PairDataset
 */

public class FASTQRecordCreator implements Function<Iterable<Tuple2<String, Long>>, String> {

    @Nullable
    public String apply(@Nullable Iterable<Tuple2<String, Long>> tuple2s) {
        // We create the data to be contained inside the record
        String seqName 		= null;
        String seq			= null;
        String qual			= null;
        String extraSeqname	= null;

        for (Tuple2<String, Long> recordLine : tuple2s) {
            // Keep in mind that records are sorted by key. This is, we have 4 available lines here
            Long lineNum = recordLine.f1;
            String line = recordLine.f0;

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
            return String.format("%s\n%s\n%s\n%s\n", seqName, seq, extraSeqname, qual);
        }
        else {
            System.err.println("Malformed record!");
            System.err.println(String.format("%s\n%s\n%s\n%s\n", seqName, seq, extraSeqname, qual));
            return null;
        }
    }

    /*
    TODO: delete
    @Override
    public String call(Iterable<Tuple2<String, Long>> iterableRecord) throws Exception {
        // We create the data to be contained inside the record
        String seqName 		= null;
        String seq			= null;
        String qual			= null;
        String extraSeqname	= null;

        for (Tuple2<String, Long> recordLine : iterableRecord) {
            // Keep in mind that records are sorted by key. This is, we have 4 available lines here
            Long lineNum = recordLine._2();
            String line = recordLine._1();

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
            return String.format("%s\n%s\n%s\n%s\n", seqName, seq, extraSeqname, qual);
        }
        else {
            System.err.println("Malformed record!");
            System.err.println(String.format("%s\n%s\n%s\n%s\n", seqName, seq, extraSeqname, qual));
            return null;
        }
    }
    */
}