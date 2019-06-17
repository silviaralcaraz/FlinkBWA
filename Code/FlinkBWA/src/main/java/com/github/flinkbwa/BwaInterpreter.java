package com.github.flinkbwa;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import java.util.ArrayList;
import java.util.List;

/**
 * This class communicates Flink with BWA.
 *
 * Created by silvia on 10/04/19.
 */
public class BwaInterpreter {
    private static final Log LOG = LogFactory.getLog(BwaInterpreter.class); // The LOG
    private Configuration conf; // Global Configuration
    private ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment(); // Flink environment
    private BwaOptions options; // Options for BWA
    private ExecutionConfig executionConf; // Execution configuration
    private long totalInputLength;
    private long blocksize;

    /**
     * Constructor to build the BWAInterpreter object from the Flink shell.
     * When creating a BWAInterpreter object from the Flink shell,
     * the BwaOptions and the Flink environment object need to be passed as argument.
     *
     * @param optionsFromShell     The BwaOptions object initialized with the user options
     * @param executionEnvironment The Flink environment from the Flink Shell.
     * @return The BWAInterpreter object with its options initialized.
     */
    public BwaInterpreter(BwaOptions optionsFromShell, ExecutionEnvironment executionEnvironment) {
        this.options = optionsFromShell;
        this.environment = executionEnvironment;
        this.initInterpreter();
    }

    /**
     * Constructor to build the BWAInterpreter object from within FlinkBWA
     *
     * @param args Arguments got from Linux console when launching FlinkBWA with Flink
     * @return The BWAInterpreter object with its options initialized.
     */
    public BwaInterpreter(String[] args) {
        this.options = new BwaOptions(args);
        this.initInterpreter();
    }

    /**
     * Method to get the length from the FASTQ input or inputs. It is set in the class variable totalInputLength
     */
    private void setTotalInputLength() {
        try {
            // Get the FileSystem
            FileSystem fs = FileSystem.get(this.conf);
            // To get the input files sizes
            ContentSummary cSummaryFile1 = fs.getContentSummary(new Path(options.getInputPath()));

            long lengthFile1 = cSummaryFile1.getLength();
            long lengthFile2 = 0;

            if (!options.getInputPath2().isEmpty()) {
                ContentSummary cSummaryFile2 = fs.getContentSummary(new Path(options.getInputPath()));
                lengthFile2 = cSummaryFile2.getLength();
            }

            // Total size. Depends on paired or single reads
            this.totalInputLength = lengthFile1 + lengthFile2;
            fs.close();
        } catch (IOException e) {
            LOG.error(e.toString());
            e.printStackTrace();
        }
    }

    /**
     * Method to create the output folder in HDFS
     */
    private void createOutputFolder() {
        try {
            FileSystem fs = FileSystem.get(this.conf);
            // Path variable
            Path outputDir = new Path(options.getOutputPath());
            // Directory creation
            if (!fs.exists(outputDir)) {
                fs.mkdirs(outputDir);
            } else {
                fs.delete(outputDir, true);
                fs.mkdirs(outputDir);
            }
            fs.close();
        } catch (IOException e) {
            LOG.error(e.toString());
            e.printStackTrace();
        }
    }

    /**
     * Function to load a FASTQ file from HDFS into a DataSet<Tuple2<Long, String>
     *
     * @param environment The Flink environment to use
     * @param pathToFastq The path to the FASTQ file
     * @return A DataSet containing <Tuple2<Long Read ID, String Read>>
     */
    public static DataSet<Tuple2<Long, String>> loadFastq(ExecutionEnvironment environment, String pathToFastq) {
        DataSet<String> fastqLines = environment.readTextFile("hdfs://" + pathToFastq);

        // Determine which FASTQ record the line belongs to.
        DataSet<Tuple2<Long, Tuple2<Long, String>>> fastqLinesByRecordNum
                = DataSetUtils.zipWithIndex(fastqLines).map(new FASTQRecordGrouper());

        // Group (by the first field of the tuple) the lines which belongs to the same record, and concatenate them into a record.
        return fastqLinesByRecordNum.groupBy(0).reduceGroup(new FASTQRecordCreator());
    }

    /**
     * Method to perform and handle the single reads sorting
     *
     * @return A Dataset containing the strings with the sorted reads from the FASTQ file
     */
    private DataSet<String> handleSingleReadsSorting() {
        DataSet<String> readsDataSet = null;
        long startTime = System.nanoTime();
        LOG.info("[" + this.getClass().getName() + "] :: Not sorting in HDFS. Timing: " + startTime);

        // Read the FASTQ file from HDFS using the FastqInputFormat class
        DataSet<Tuple2<Long, String>> singleReadsKeyVal = loadFastq(this.environment, this.options.getInputPath());

        // Sort in memory with no partitioning
        if ((options.getPartitionNumber() == 0) && (options.isSortFastqReads())) {
            readsDataSet = singleReadsKeyVal.sortPartition(new KeySelectorSingle(), Order.ASCENDING).map(new BwaMapFunctionValues());
            LOG.info("[" + this.getClass().getName() + "] :: Repartition with sort");
        }

        // Sort in memory with partitioning
        else if ((options.getPartitionNumber() != 0) && (options.isSortFastqReads())) {
            // First, the partition operation is performed, after that, a sort.
            readsDataSet = singleReadsKeyVal.rebalance().setParallelism(options.getPartitionNumber()).map(new BwaMapFunctionValues());
            // About rebalance operation: partitions elements round-robin, creating equal load per partition.
            LOG.info("[" + this.getClass().getName() + "] :: Sorting in memory with partitioning");
        }

        // No Sort with no partitioning
        else if ((options.getPartitionNumber() == 0) && (!options.isSortFastqReads())) {
            readsDataSet = singleReadsKeyVal.map(new BwaMapFunctionValues());
            LOG.info("[" + this.getClass().getName() + "] :: No sort and no partitioning");
        }

        // No Sort with partitioning
        else {
            LOG.info("[" + this.getClass().getName() + "] :: No sort with partitioning");
            int numPartitions = singleReadsKeyVal.getExecutionEnvironment().getParallelism();
            /*
             * As in previous cases, the coalesce operation is not suitable
             * if we want to achieve the maximum speedup, so, repartition
             * is used.
             */
            if ((numPartitions) <= options.getPartitionNumber()) {
                LOG.info("[" + this.getClass().getName() + "] :: Repartition with no sort");
            } else {
                LOG.info("[" + this.getClass().getName() + "] :: Repartition(Coalesce) with no sort");
            }
            readsDataSet = singleReadsKeyVal.rebalance().setParallelism(options.getPartitionNumber()).map(new BwaMapFunctionValues());
        }
        long endTime = System.nanoTime();
        LOG.info("[" + this.getClass().getName() + "] :: End of sorting. Timing: " + endTime);
        LOG.info("[" + this.getClass().getName() + "] :: Total time: " + (endTime - startTime) / 1e9 / 60.0 + " minutes");

        return readsDataSet;
    }

    /**
     * Method to perform and handle the paired reads sorting
     *
     * @return A JavaRDD containing grouped reads from the paired FASTQ files
     */
    private DataSet<Tuple2<String, String>> handlePairedReadsSorting() {
        DataSet<Tuple2<String, String>> readsDataSet = null;
        long startTime = System.nanoTime();
        LOG.info("[" + this.getClass().getName() + "] :: Not sorting in HDFS. Timing: " + startTime);

        // Read the two FASTQ files from HDFS using the loadFastq method.
        DataSet<Tuple2<Long, String>> datasetTmp1 = loadFastq(this.environment, options.getInputPath());
        DataSet<Tuple2<Long, String>> datasetTmp2 = loadFastq(this.environment, options.getInputPath2());

        // Flink join operation to combine the datasets.
        DataSet<Tuple2<Long, Tuple2<String, String>>> pairedReadsDataSet = datasetTmp1.join(datasetTmp2).
                where(new KeySelectorSingle()).equalTo(new KeySelectorSingle()).map(new FASTQPairMapOperator());

        // Sort in memory with no partitioning
        if ((options.getPartitionNumber() == 0) && (options.isSortFastqReads())) {
            readsDataSet = pairedReadsDataSet.sortPartition(new KeySelectorPaired(), Order.ASCENDING).map(new BwaMapFunctionPairValues());
            LOG.info("[" + this.getClass().getName() + "] :: Sorting in memory without partitioning");
        }

        // Sort in memory with partitioning
        else if ((options.getPartitionNumber() != 0) && (options.isSortFastqReads())) {
            readsDataSet = pairedReadsDataSet.rebalance().setParallelism(options.getPartitionNumber()).
                    sortPartition(new KeySelectorPaired(), Order.ASCENDING).map(new BwaMapFunctionPairValues());
            LOG.info("[" + this.getClass().getName() + "] :: Repartition with sort");
        }

        // No Sort with no partitioning
        else if ((options.getPartitionNumber() == 0) && (!options.isSortFastqReads())) {
            LOG.info("[" + this.getClass().getName() + "] :: No sort and no partitioning");
            // So, we only transform the Dataset<Tuple2<Long, Tuple2<String, String>>> into a Dataset<Tuple2<String, String>>
            readsDataSet = pairedReadsDataSet.map(new BwaMapFunctionPairValues());
        }

        // No Sort with partitioning
        else {
            LOG.info("[" + this.getClass().getName() + "] :: No sort with partitioning");
            int numPartitions = pairedReadsDataSet.getExecutionEnvironment().getParallelism();
            /*
             * As in previous cases, the coalesce operation is not suitable
             * if we want to achieve the maximum speedup, so, repartition
             * is used.
             */
            if ((numPartitions) <= options.getPartitionNumber()) {
                LOG.info("[" + this.getClass().getName() + "] :: Repartition with no sort");
            } else {
                LOG.info("[" + this.getClass().getName() + "] :: Repartition(Coalesce) with no sort");
            }

            readsDataSet = pairedReadsDataSet.rebalance().setParallelism(options.getPartitionNumber()).map(new BwaMapFunctionPairValues());
        }
        long endTime = System.nanoTime();
        LOG.info("[" + this.getClass().getName() + "] :: End of sorting. Timing: " + endTime);
        LOG.info("[" + this.getClass().getName() + "] :: Total time: " + (endTime - startTime) / 1e9 / 60.0 + " minutes");

        return readsDataSet;
    }

    /**
     * Procedure to perform the alignment using paired reads
     *
     * @param bwa          The Bwa object to use
     * @param readsDataSet The DataSet containing the paired reads
     * @return A list of strings containing the resulting sam files where the output alignments are stored
     */
    private List<String> MapPairedBwa(Bwa bwa, DataSet<Tuple2<String, String>> readsDataSet) {
        List<String> result = new ArrayList<>();
        try {
            List<ArrayList<String>> collectOutput = readsDataSet
                    .mapPartition(new BwaPairedAlignment(readsDataSet.getExecutionEnvironment(), bwa))
                    .collect();

            for (ArrayList<String> arrayList : collectOutput) {
                for (String string : arrayList) {
                    result.add(string);
                }
            }

            return result;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * @param bwa          The Bwa object to use
     * @param readsDataSet The Dataset containing the reads
     * @return A list of strings containing the resulting sam files where the output alignments are stored
     */
    private List<String> MapSingleBwa(Bwa bwa, DataSet<String> readsDataSet) {
        List<String> result = new ArrayList<>();
        try {
            List<ArrayList<String>> collectOutput = readsDataSet
                    .mapPartition(new BwaSingleAlignment(readsDataSet.getExecutionEnvironment(), bwa))
                    .collect();

            for (ArrayList<String> arrayList : collectOutput) {
                for (String string : arrayList) {
                    result.add(string);
                }
            }

            return result;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * Runs BWA with the specified options
     *
     * @brief This function runs BWA with the input data selected and with the options also selected
     * by the user.
     */
    public void runBwa() {
        LOG.info("[" + this.getClass().getName() + "] :: Starting BWA");
        Bwa bwa = new Bwa(this.options);
        List<String> returnedValues = null;

        if (bwa.isPairedReads()) {
            DataSet<Tuple2<String, String>> readsDataSet = handlePairedReadsSorting();
            returnedValues = MapPairedBwa(bwa, readsDataSet);
        } else {
            DataSet<String> readsDataSet = handleSingleReadsSorting();
            returnedValues = MapSingleBwa(bwa, readsDataSet);
        }

        // In the case of use a reducer the final output has to be stored in just one file
        if (this.options.getUseReducer()) {
            try {
                FileSystem fs = FileSystem.get(this.conf);
                Path finalHdfsOutputFile = new Path(this.options.getOutputHdfsDir() + "/FullOutput.sam");
                FSDataOutputStream outputFinalStream = fs.create(finalHdfsOutputFile, true);

                // We iterate over the resulting files in HDFS and agregate them into only one file.
                for (int i = 0; i < returnedValues.size(); i++) {
                    LOG.info("FlinkBWA :: Returned file ::" + returnedValues.get(i));
                    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(returnedValues.get(i)))));
                    String line;
                    line = br.readLine();

                    while (line != null) {
                        if (i == 0 || !line.startsWith("@")) {
                            outputFinalStream.write((line + "\n").getBytes());
                        }
                        line = br.readLine();
                    }
                    br.close();
                    fs.delete(new Path(returnedValues.get(i)), true);
                }
                outputFinalStream.close();
                fs.close();
            } catch (Exception e) {
                e.printStackTrace();
                LOG.error(e.toString());
            }
        }
    }

    /**
     * Procedure to init the BWAInterpreter configuration parameters
     */
    public void initInterpreter() {
        //If ctx is null, this procedure is being called from the Linux console with Flink
        if (this.environment == null) {
            String sorting;
            //Check for the options to perform the sort reads
            if (options.isSortFastqReads()) {
                sorting = "SortFlink";
            } else if (options.isSortFastqReadsHdfs()) {
                sorting = "SortHDFS";
            } else {
                sorting = "NoSort";
            }
            this.environment = ExecutionEnvironment.getExecutionEnvironment();
        } else {
            this.executionConf = this.environment.getConfig();
        }
        //The Hadoop configuration is obtained
        this.conf = new Configuration();

        //The block size
        this.blocksize = this.conf.getLong("dfs.blocksize", 134217728);
        createOutputFolder();
        setTotalInputLength();
    }
}