package com.github.flinkbwa;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.flink.api.java.hadoop.mapred.utils.HadoopUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.flink.api.java.ExecutionEnvironment;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by silvia on 10/04/19.
 */
public class BwaAlignmentBase {
    protected static final Log LOG = LogFactory.getLog(BwaAlignmentBase.class);

    protected String appName = "";
    protected String appId = "";
    protected String tmpDir = "";
    protected Bwa bwaInterpreter;

    /**
     * Constructor for this class
     *
     * @param environment        The ExecutionEnviroment to use
     * @param bwaInterpreter The Bwa object used to perform the alignment
     * @brief This constructor creates a BwaAlignment object to process in each one of the mappers
     */
    public BwaAlignmentBase(ExecutionEnvironment environment, Bwa bwaInterpreter) {
        this.appId = environment.getId().toString();
        this.appName = "FlinkBWA";
        //FIXME: change tmpdir?
        this.tmpDir = "/tmp";
        //GlobalConfiguration.FLINK_CONF_FILENAME;
        //this.tmpDir = context.getLocalProperty("spark.local.dir");
        this.bwaInterpreter = bwaInterpreter;

        //We set the tmp dir
        if ((this.tmpDir == null || this.tmpDir == "null")
                && this.bwaInterpreter.getTmpDir() != null
                && !this.bwaInterpreter.getTmpDir().isEmpty()) {

            this.tmpDir = bwaInterpreter.getTmpDir();
        }

        if (this.tmpDir == null || this.tmpDir == "null") {
            this.tmpDir = HadoopUtils.getHadoopConfiguration().get("hadoop.tmp.dir");
        }

        if (this.tmpDir.startsWith("file:")) {
            this.tmpDir = this.tmpDir.replaceFirst("file:", "");
        }

        File tmpFileDir = new File(this.tmpDir);

        if (!tmpFileDir.isDirectory() || !tmpFileDir.canWrite()) {
            this.tmpDir = "/tmp/";
        }
        this.LOG.info("[" + this.getClass().getName() + "] :: " + this.appId + " - " + this.appName);
    }

    /**
     * Function that performs an actual alignment
     *
     * @param outputSamFileName String representing the output sam location
     * @param fastqFileName1    String representing the first of the FASTQ files
     * @param fastqFileName2    String representing the second of the FASTQ files. If single reads, it can be empty.
     */
    public void alignReads(String outputSamFileName, String fastqFileName1, String fastqFileName2) {
        // First, the two input FASTQ files are set
        this.bwaInterpreter.setInputFile(fastqFileName1);

        if (this.bwaInterpreter.isPairedReads()) {
            bwaInterpreter.setInputFile2(fastqFileName2);
        }
        if (this.tmpDir.lastIndexOf("/") == this.tmpDir.length() - 1) {
            this.bwaInterpreter.setOutputFile(this.tmpDir + outputSamFileName);
        } else {
            this.bwaInterpreter.setOutputFile(this.tmpDir + "/" + outputSamFileName);
        }


        //We run BWA with the corresponding options set
        this.bwaInterpreter.run(0);

        //In case of the ALN algorithm, more executions of BWA are needed
        if (this.bwaInterpreter.isAlnAlgorithm()) {

            //The next execution of BWA in the case of ALN algorithm
            this.bwaInterpreter.run(1);

            //Finally, if we are talking about paired reads and aln algorithm, a final execution is needed
            if (this.bwaInterpreter.isPairedReads()) {
                this.bwaInterpreter.run(2);

                //Delete .sai file number 2
                File tmpSaiFile2 = new File(fastqFileName2 + ".sai");
                tmpSaiFile2.delete();
            }

            //Delete *.sai file number 1
            File tmpSaiFile1 = new File(fastqFileName1 + ".sai");
            tmpSaiFile1.delete();
        }
    }

    /**
     * Function that copy local sam results to HDFS
     *
     * @param outputSamFileName The output where the final results will be stored
     * @return An ArrayList containing all the file locations
     */
    public Iterator<String> copyResults(String outputSamFileName) {
        ArrayList<String> returnedValues = new ArrayList<String>();
        String outputDir = this.bwaInterpreter.getOutputHdfsDir();

        this.LOG.info("[" + this.getClass().getName() + "] :: " + this.appId + " - " + this.appName + " Copying files...");

        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            fs.copyFromLocalFile(
                    new Path(this.bwaInterpreter.getOutputFile()),
                    new Path(outputDir + "/" + outputSamFileName)
            );
        } catch (IOException e) {
            e.printStackTrace();
            this.LOG.error(e.toString());
        }

        // Delete the old results file
        File tmpSamFullFile = new File(this.bwaInterpreter.getOutputFile());
        tmpSamFullFile.delete();

        returnedValues.add(outputDir + "/" + outputSamFileName);

        return returnedValues.iterator();
    }

    /**
     * @param readBatchID Identification for the sam file
     * @return A String for the sam file name
     */
    public String getOutputSamFilename(Long readBatchID) {
        return this.appName + "-" + this.appId + "-" + readBatchID + ".sam";
    }

    /**
     * @param readStreamID    Identification for the sam file
     * @param fastqFileName1 First of the FASTQ files
     * @param fastqFileName2 Second of the FASTQ files
     * @return
     */
    public Iterator<String> runAlignmentProcess(Long readStreamID, String fastqFileName1, String fastqFileName2) {
        //The output filename (without the tmp directory)
        String outputSamFileName = this.getOutputSamFilename(readStreamID);
        this.alignReads(outputSamFileName, fastqFileName1, fastqFileName2);

        // Copy the result to HDFS
        return this.copyResults(outputSamFileName);
    }
}
