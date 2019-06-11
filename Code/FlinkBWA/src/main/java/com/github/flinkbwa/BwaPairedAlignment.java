package com.github.flinkbwa;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.*;
import java.util.ArrayList;

/**
 * Class to perform the alignment over a split from the Dataset of paired reads.
 *
 * Created by silvia on 10/04/19.
 */
public class BwaPairedAlignment extends BwaAlignmentBase implements MapPartitionFunction<Tuple2<String, String>, ArrayList<String>> {

    /**
     * Constructor
     * @param environment The Flink environment
     * @param bwaInterpreter The BWA interpreter object to use
     */
    public BwaPairedAlignment(ExecutionEnvironment environment, Bwa bwaInterpreter) {
        super(environment, bwaInterpreter);
    }

    /**
     * Code to run in each one of the mappers. This is, the alignment with the corresponding entry
     * data The entry data has to be written into the local filesystem
     * @param input An iterator containing the values in this Dataset
     * @param output A collector containing the sam file name generated
     * @throws Exception
     */
    public void mapPartition(Iterable<Tuple2<String, String>> input, Collector<ArrayList<String>> output) throws Exception {
        // STEP 1: Input fastq reads tmp file creation
        LOG.info("["+this.getClass().getName()+"] :: Tmp dir: " + this.tmpDir);

        String fastqFileName1;
        String fastqFileName2;
        Long id = System.currentTimeMillis();

        if(this.tmpDir.lastIndexOf("/") == this.tmpDir.length()-1) {
            fastqFileName1 = this.tmpDir + this.appId + "-Dataset" + id + "_1";
            fastqFileName2 = this.tmpDir + this.appId + "-Dataset" + id + "_2";
        }
        else {
            fastqFileName1 = this.tmpDir + "/" + this.appId + "-Dataset" + id + "_1";
            fastqFileName2 = this.tmpDir + "/" + this.appId + "-Dataset" + id + "_2";
        }

        LOG.info("["+this.getClass().getName()+"] :: Writing file: " + fastqFileName1);
        LOG.info("["+this.getClass().getName()+"] :: Writing file: " + fastqFileName2);

        File FastqFile1 = new File(fastqFileName1);
        File FastqFile2 = new File(fastqFileName2);

        FileOutputStream fos1;
        FileOutputStream fos2;

        BufferedWriter bw1;
        BufferedWriter bw2;

        //We write the data contained in this split into the two tmp files
        try {
            fos1 = new FileOutputStream(FastqFile1);
            fos2 = new FileOutputStream(FastqFile2);

            bw1 = new BufferedWriter(new OutputStreamWriter(fos1));
            bw2 = new BufferedWriter(new OutputStreamWriter(fos2));

            for(Tuple2<String, String> tuple: input){
                bw1.write(tuple.f0);
                bw1.newLine();

                bw2.write(tuple.f1);
                bw2.newLine();
            }

            bw1.close();
            bw2.close();

            input = null;

            // This is where the actual local alignment takes place
            output.collect(this.runAlignmentProcess(id, fastqFileName1, fastqFileName2));

            // Delete temporary files, as they have now been copied to the output directory
            LOG.info("["+this.getClass().getName()+"] :: Deleting file: " + fastqFileName1);
            FastqFile1.delete();

            LOG.info("["+this.getClass().getName()+"] :: Deleting file: " + fastqFileName2);
            FastqFile2.delete();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            LOG.error("["+this.getClass().getName()+"] "+e.toString());
        }
    }
}