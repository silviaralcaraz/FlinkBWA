package com.github.flinkbwa;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.api.java.hadoop.mapred.utils.HadoopUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * BWAInterpreter class
 */
public class BwaInterpreter {
    //private JavaRDD<Tuple2<String, String>> dataRDD; // Nunca se usa en SparkBWA
    //private SparkConf sparkConf; 	// The Spark Configuration to use
    //private JavaSparkContext 	ctx; // The Java Spark Context
    //private String inputTmpFileName; // We do not have tmp files in HDFS
    private static final Log LOG = LogFactory.getLog(BwaInterpreter.class); // The LOG
    private Configuration conf; // Global Configuration
    private org.apache.flink.configuration.Configuration flinkConf; // The Flink Configuration to use
    private ExecutionConfig executionConf; // Execution configuration
    private ParameterTool parameters; // Flink job parameters
    private ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment(); // Flink environment
    private BwaOptions options; // Options for BWA
    private long totalInputLength;
    private long blocksize;

    /**
     * Constructor to build the BWAInterpreter object from the Flink shell When creating a
     * BWAInterpreter object from the Flink shell, the BwaOptions and the Spark Context objects need
     * to be passed as argument.
     *
     * @param optionsFromShell     The BwaOptions object initialized with the user options
     * @param executionEnvironment The Spark Context from the Spark Shell. Usually "sc"
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
     * @param args Arguments got from Linux console when launching SparkBWA with Spark
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
     * @param environment The JavaSparkContext to use
     * @param pathToFastq The path to the FASTQ file
     * @return A DataSet containing <Tuple2<Long Read ID, String Read>>
     */
    public static DataSet<Tuple2<Long, String>> loadFastq(ExecutionEnvironment environment, String pathToFastq) {
        DataSet<String> fastqLines = environment.readTextFile(pathToFastq);
        // If is a hdfs file:
        //environment.readCsvFile("hdfs:" + pathToFastq);

        // Determine which FASTQ record the line belongs to.
        DataSet<Tuple2<Long, Tuple2<Long, String>>> fastqLinesByRecordNum
                = DataSetUtils.zipWithIndex(fastqLines).map(new FASTQRecordGrouper());

        // Group group the lines which belongs to the same record, and concatinate them into a record.
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
            readsDataSet = singleReadsKeyVal.sortPartition(0, Order.ASCENDING).map(new BwaMapFunctionValues());
            //TODO: delete next line if the previous works.
            //readsDataSet = singleReadsKeyVal.sortPartition(0, Order.ASCENDING).setParallelism(1).map(new BwaMapFunctionValues());
            LOG.info("[" + this.getClass().getName() + "] :: Repartition with sort");
        }

        // Sort in memory with partitioning
        else if ((options.getPartitionNumber() != 0) && (options.isSortFastqReads())) {
            // First, the partition operation is performed, after that, a sort.
            readsDataSet = singleReadsKeyVal.partitionByRange(0).sortPartition(0, Order.ASCENDING).
                    map(new BwaMapFunctionValues());
            LOG.info("[" + this.getClass().getName() + "] :: Sorting in memory without partitioning");
        }

        // No Sort with no partitioning
        else if ((options.getPartitionNumber() == 0) && (!options.isSortFastqReads())) {
            LOG.info("[" + this.getClass().getName() + "] :: No sort and no partitioning");
            readsDataSet = singleReadsKeyVal.map(new BwaMapFunctionValues());
        }

        // No Sort with partitioning
        else {
            LOG.info("[" + this.getClass().getName() + "] :: No sort with partitioning");
            readsDataSet = singleReadsKeyVal.partitionByRange(0).map(new BwaMapFunctionValues());
            //FIXME: if level of parallelism is not the same as partitions size
            //int numPartitions = singleReadsKeyVal.partitions().size();
            int numPartitions = singleReadsKeyVal.getExecutionEnvironment().getParallelism();

            /*
             * As in previous cases, the coalesce operation is not suitable
			 * if we want to achieve the maximum speedup, so, repartition
			 * is used.
			 */
            if ((numPartitions) <= options.getPartitionNumber()) {
                LOG.info("["+this.getClass().getName()+"] :: Repartition with no sort");
            }
            else {
                LOG.info("["+this.getClass().getName()+"] :: Repartition(Coalesce) with no sort");
            }
            //reads = singleReadsKeyVal.repartition(options.getPartitionNumber()).values();
            readsDataSet = singleReadsKeyVal.partitionByRange(options.getPartitionNumber()).
                    map(new BwaMapFunctionValues());
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
        LOG.info("[" + this.getClass().getName() + "] ::Not sorting in HDFS. Timing: " + startTime);

        // Read the two FASTQ files from HDFS using the loadFastq method. After that, a Spark join operation is performed
        DataSet<Tuple2<Long, String>> datasetTmp1 = loadFastq(this.environment, options.getInputPath());
        DataSet<Tuple2<Long, String>> datasetTmp2 = loadFastq(this.environment, options.getInputPath2());
        DataSet<Tuple2<Long, Tuple2<String, String>>> pairedReadsDataSet = datasetTmp1.join(datasetTmp2).
                where(new FASTQKeySelector()).equalTo(new FASTQKeySelector()).map(new FASTQPairMapOperator());

        /*
        En flink no existe esta funcion, el programa debe ser recargado para actualizar los datos
        FIXME: delete if is not necessary (I think in Flink doesn't exist) || change by the right methods
        datasetTmp1.unpersist();
        datasetTmp2.unpersist();
        */

        // Sort in memory with no partitioning
        if ((options.getPartitionNumber() == 0) && (options.isSortFastqReads())) {
            readsDataSet = pairedReadsDataSet.sortPartition(0, Order.ASCENDING).
                    map(new BwaMapFunctionPairValues());
            LOG.info("[" + this.getClass().getName() + "] :: Sorting in memory without partitioning");
        }

        // Sort in memory with partitioning
        else if ((options.getPartitionNumber() != 0) && (options.isSortFastqReads())) {
            //pairedReadsDataSet = pairedReadsDataSet.repartition(options.getPartitionNumber());
            readsDataSet = pairedReadsDataSet.partitionByRange(options.getPartitionNumber()).sortPartition(0, Order.ASCENDING).
                    map(new BwaMapFunctionPairValues());
            LOG.info("[" + this.getClass().getName() + "] :: Repartition with sort");
        }

        // No Sort with no partitioning
        else if ((options.getPartitionNumber() == 0) && (!options.isSortFastqReads())) {
            LOG.info("[" + this.getClass().getName() + "] :: No sort and no partitioning");
        }

        // No Sort with partitioning
        else {
            LOG.info("[" + this.getClass().getName() + "] :: No sort with partitioning");
            //int numPartitions = pairedReadsRDD.partitions().size();
            int numPartitions = pairedReadsDataSet.getExecutionEnvironment().getParallelism();
			/*
			 * As in previous cases, the coalesce operation is not suitable
			 * if we want to achieve the maximum speedup, so, repartition
			 * is used.
			 */

            if ((numPartitions) <= options.getPartitionNumber()) {
                LOG.info("["+this.getClass().getName()+"] :: Repartition with no sort");
            }
            else {
                LOG.info("["+this.getClass().getName()+"] :: Repartition(Coalesce) with no sort");
            }

            readsDataSet = pairedReadsDataSet.partitionByRange(options.getPartitionNumber()).
                    map(new BwaMapFunctionPairValues());
            /*
            readsRDD = pairedReadsRDD
                    .repartition(options.getPartitionNumber())
                    .values();
             */
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
        // The mapPartitionsWithIndex is used over this RDD to perform the alignment.
        try {
            Iterator iterator = readsDataSet.mapPartition(
                    new BwaPairedAlignment(readsDataSet.getExecutionEnvironment(), bwa))
                    .collect().iterator();
            List samFilenamesList = IteratorUtils.toList(iterator);
            // The resulting sam filenames are returned
            return samFilenamesList;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new ArrayList<String>();

    }

    /**
     * @param bwa          The Bwa object to use
     * @param readsDataSet The RDD containing the paired reads
     * @return A list of strings containing the resulting sam files where the output alignments are stored
     */
    private List<String> MapSingleBwa(Bwa bwa, DataSet<String> readsDataSet) {
        try {
            // The mapPartitionsWithIndex is used over this RDD to perform the alignment.
            Iterator iterator = readsDataSet.mapPartition(
                    new BwaSingleAlignment(readsDataSet.getExecutionEnvironment(), bwa))
                    .collect().iterator();
            List samFilenamesList = IteratorUtils.toList(iterator);
            // The resulting sam filenames are returned
            return samFilenamesList;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new ArrayList<String>();
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
        List<String> returnedValues;

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
                    LOG.info("JMAbuin:: FlinkBWA :: Returned file ::" + returnedValues.get(i));
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

                environment.execute("FlinkBWA");
            } catch (Exception e) {
                e.printStackTrace();
                LOG.error(e.toString());
            }
        }

    }

    /**
     * Procedure to init the BWAInterpreter configuration parameters
     */
    //FIXME: fix it if not works fine
    public void initInterpreter() {
        //If ctx is null, this procedure is being called from the Linux console with Spark
        if (this.environment == null) {
            String sorting;
            //Check for the options to perform the sort reads
            if (options.isSortFastqReads()) {
                sorting = "SortSpark";
            } else if (options.isSortFastqReadsHdfs()) {
                sorting = "SortHDFS";
            } else {
                sorting = "NoSort";
            }

            //The application name is set
            /*
            this.sparkConf = new SparkConf().setAppName("SparkBWA_"
                    + options.getInputPath().split("/")[options.getInputPath().split("/").length - 1]
                    + "-"
                    + options.getPartitionNumber()
                    + "-"
                    + sorting);
            */
            /*this.flinkConf = new org.apache.flink.configuration.Configuration();
            this.flinkConf.setString("AppName", "FlinkBWA_"
                    + options.getInputPath().split("/")[options.getInputPath().split("/").length - 1]
                    + "-"
                    + options.getPartitionNumber()
                    + "-"
                    + sorting);

            //The ctx is created from scratch
            this.environment = new LocalEnvironment(this.flinkConf);*/
            //this.ctx = new JavaSparkContext(this.sparkConf);
            this.environment = ExecutionEnvironment.getExecutionEnvironment();
        }
        //Otherwise, the procedure is being called from the Spark shell
        else {
            //this.sparkConf = this.ctx.getConf();
            this.executionConf = this.environment.getConfig();
        }
        //The Hadoop configuration is obtained
        this.conf = new Configuration();
        //this.conf = this.ctx.hadoopConfiguration();
        //this.conf = HadoopUtils.getHadoopConfiguration(); // Need hadoop especification in flink-conf.yaml (?)

        //The block size
        this.blocksize = this.conf.getLong("dfs.blocksize", 134217728);
        createOutputFolder();
        setTotalInputLength();
    }
}