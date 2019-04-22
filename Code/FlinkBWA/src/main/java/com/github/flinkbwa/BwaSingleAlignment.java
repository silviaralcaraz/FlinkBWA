/**
 * Copyright 2016 José Manuel Abuín Mosquera <josemanuel.abuin@usc.es>
 * <p>
 * <p>This file is part of SparkBWA.
 * <p>
 * <p>SparkBWA is free software: you can redistribute it and/or modify it under the terms of the GNU
 * General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * <p>SparkBWA is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 * <p>
 * <p>You should have received a copy of the GNU General Public License along with SparkBWA. If not,
 * see <http://www.gnu.org/licenses/>.
 */
package com.github.flinkbwa;

import java.io.*;
import java.util.Iterator;

//import org.apache.spark.SparkContext;
//import org.apache.spark.api.java.function.Function2;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.util.Collector;

/**
 * Class to perform the alignment over a split from the Dataset of single reads
 */
public class BwaSingleAlignment extends BwaAlignmentBase implements MapPartitionFunction<String, Iterator<String>> {
    /**
     * Constructor
     *
     * @param environment    The Flink execution environment
     * @param bwaInterpreter The BWA interpreter object to use
     */
    public BwaSingleAlignment(ExecutionEnvironment environment, Bwa bwaInterpreter) {
        super(environment, bwaInterpreter);
    }

    public void mapPartition(Iterable<String> iterable, Collector<Iterator<String>> collector) throws Exception {
        LOG.info("[" + this.getClass().getName() + "] :: Tmp dir: " + this.tmpDir);
        String fastqFileName1;
        Long streamID = System.currentTimeMillis();

        if (this.tmpDir.lastIndexOf("/") == this.tmpDir.length() - 1) {
            fastqFileName1 = this.tmpDir + this.appId + "-Dataset" + streamID + "_1";
        } else {
            fastqFileName1 = this.tmpDir + "/" + this.appId + "-Dataset" + streamID + "_1";
        }
        LOG.info("[" + this.getClass().getName() + "] :: Writing file: " + fastqFileName1);

        File FastqFile1 = new File(fastqFileName1);
        FileOutputStream fos1;
        BufferedWriter bw1;
        try {
            fos1 = new FileOutputStream(FastqFile1);
            bw1 = new BufferedWriter(new OutputStreamWriter(fos1));

            Iterator<String> data = iterable.iterator();
            while (data.hasNext()) {
                String record = data.next();
                bw1.write(record);
                bw1.newLine();
            }

            bw1.flush();
            bw1.close();

            // This is where the actual local alignment takes place
            collector.collect(this.runAlignmentProcess(streamID, fastqFileName1, null));
            // Delete the temporary file, as is have now been copied to the output directory
            FastqFile1.delete();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            LOG.error("[" + this.getClass().getName() + "] " + e.toString());
        }
    }

    /**
     * Code to run in each one of the mappers. This is, the alignment with the corresponding entry
     * data The entry data has to be written into the local filesystem
     * @param arg0 The RDD Id
     * @param arg1 An iterator containing the values in this RDD
     * @return An iterator containing the sam file name generated
     * @throws Exception
     */
    /* TODO: delete this method
    public Iterator<String> call(Integer arg0, Iterator<String> arg1) throws Exception {
        LOG.info("[" + this.getClass().getName() + "] :: Tmp dir: " + this.tmpDir);
        String fastqFileName1;
        if (this.tmpDir.lastIndexOf("/") == this.tmpDir.length() - 1) {
            fastqFileName1 = this.tmpDir + this.appId + "-RDD" + arg0 + "_1";
        } else {
            fastqFileName1 = this.tmpDir + "/" + this.appId + "-RDD" + arg0 + "_1";
        }

        LOG.info("[" + this.getClass().getName() + "] :: Writing file: " + fastqFileName1);
        File FastqFile1 = new File(fastqFileName1);
        FileOutputStream fos1;
        BufferedWriter bw1;
        ArrayList<String> returnedValues = new ArrayList<String>();

        try {
            fos1 = new FileOutputStream(FastqFile1);
            bw1 = new BufferedWriter(new OutputStreamWriter(fos1));
            String newFastqRead;

            while (arg1.hasNext()) {
                newFastqRead = arg1.next();
                bw1.write(newFastqRead);
                bw1.newLine();
            }
            bw1.close();
            //We do not need the input data anymore, as it is written in a local file
            arg1 = null;
            // This is where the actual local alignment takes place
            returnedValues = this.runAlignmentProcess(arg0, fastqFileName1, null);
            // Delete the temporary file, as is have now been copied to the output directory
            FastqFile1.delete();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            LOG.error("[" + this.getClass().getName() + "] " + e.toString());
        }
        return returnedValues.iterator();
    }
     */
}