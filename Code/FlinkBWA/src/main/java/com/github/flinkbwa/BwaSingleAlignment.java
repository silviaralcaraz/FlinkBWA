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
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.util.Collector;

/**
 * Class to perform the alignment over a split from the Dataset of single reads
 */
public class BwaSingleAlignment extends BwaAlignmentBase implements MapPartitionFunction<String, ArrayList<String>> {
    /**
     * Constructor
     *
     * @param environment    The Flink execution environment
     * @param bwaInterpreter The BWA interpreter object to use
     */
    public BwaSingleAlignment(ExecutionEnvironment environment, Bwa bwaInterpreter) {
        super(environment, bwaInterpreter);
    }

    /**
     * Code to run in each one of the mappers. This is, the alignment with the corresponding entry
     * data The entry data has to be written into the local filesystem
     *
     * @param input  An iterator contanining the values in this Dataset
     * @param output An collector containing the sam file name generated
     * @throws Exception
     */
    @Override
    public void mapPartition(Iterable<String> input, Collector<ArrayList<String>> output) throws Exception {
        LOG.info("[" + this.getClass().getName() + "] :: Tmp dir: " + this.tmpDir);
        String fastqFileName1;
        Long id = System.currentTimeMillis();

        if (this.tmpDir.lastIndexOf("/") == this.tmpDir.length() - 1) {
            fastqFileName1 = this.tmpDir + this.appId + "-Dataset" + id + "_1";
        } else {
            fastqFileName1 = this.tmpDir + "/" + this.appId + "-Dataset" + id + "_1";
        }
        LOG.info("[" + this.getClass().getName() + "] :: Writing file: " + fastqFileName1);

        File FastqFile1 = new File(fastqFileName1);
        FileOutputStream fos1;
        BufferedWriter bw1;

        try {
            fos1 = new FileOutputStream(FastqFile1);
            bw1 = new BufferedWriter(new OutputStreamWriter(fos1));

            Iterator iterator = input.iterator();
            while (iterator.hasNext()) {
                String record = (String) iterator.next();
                bw1.write(record);
                bw1.newLine();
            }

            bw1.flush();
            bw1.close();

            // This is where the actual local alignment takes place
            output.collect(this.runAlignmentProcess(id, fastqFileName1, null));

            // Delete the temporary file, as is have now been copied to the output directory
            FastqFile1.delete();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            LOG.error("[" + this.getClass().getName() + "] " + e.toString());
        }
    }
}
