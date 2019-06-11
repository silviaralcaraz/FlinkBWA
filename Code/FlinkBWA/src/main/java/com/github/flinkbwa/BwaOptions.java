package com.github.flinkbwa;

import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;

/**
 * Class to parse and set the Bwa options.
 *
 * Created by silvia on 10/04/19.
 */
public class BwaOptions {
    private static final Log LOG = LogFactory.getLog(BwaOptions.class);

    private boolean memAlgorithm = true;
    private boolean alnAlgorithm = false;
    private boolean bwaswAlgorithm = false;
    private String bwaArgs = "";

    private boolean pairedReads = true;
    private boolean singleReads = false;

    private String indexPath = "";
    private String outputHdfsDir = "";
    private String inputPath = "";
    private String inputPath2 = "";
    private boolean sortFastqReads = false;
    private boolean sortFastqReadsHdfs = false;

    private String tmpPath = "";
    private boolean useReducer = false;
    private String correctUse = "flink run -c com.github.flinkbwa.FlinkBWA FlinkBWA.jar -m yarn-cluster";
    // -c: run a program with specific class as an entry point
    // -m: run a program on yarn cluster environment

    // Header to show when the program is not launched correctly
    private String header = "\t<FASTQ file 1> [FASTQ file 2] <SAM file output>\n\n" +
            "FlinkBWA performs genomic alignment using bwa in a Hadoop/YARN cluster\n" +
            "Available FlinkBWA options are:\n";

    private String headerAlt = "flink run -c com.github.flinkbwa.FlinkBWA FlinkBWA.jar\n" +
            "       [-a | -b | -m]  [-f | -k] [-h] [-i <Index prefix>]   [-n <Number of\n" +
            "       partitions>] [-p | -s] [-r]  [-w <\"BWA arguments\">] [-t <\"tmp path\">]\n" +
            "       <FASTQ file 1> [FASTQ file 2] <SAM file output>";

    // Footer to show when the program is not launched correctly
    private String footer = "\nPlease report issues at josemanuel.abuin@usc.es or silvia.rodriguez.alcaraz@rai.usc.es";
    private String outputPath = "";
    private int partitionNumber = 0;

    // Available options
    private Options options = null;

    private void printHelp() {
        HashMap<String, ArrayList<Option>> optionsTable = new HashMap<String, ArrayList<Option>>();
        // Add groups in the HashMap
        Iterator availableGroups = this.options.getOptions().iterator();
        while (availableGroups.hasNext()) {
            Option newOption = ((Option) availableGroups.next());
            if (optionsTable.containsKey(this.options.getOptionGroup(newOption).toString())) {
                optionsTable.get(this.options.getOptionGroup(newOption).toString()).add(newOption);
            } else {
                ArrayList<Option> newArrayOptions = new ArrayList<Option>();
                newArrayOptions.add(newOption);
                optionsTable.put(this.options.getOptionGroup(newOption).toString(), newArrayOptions);
            }
        }

        Iterator it = optionsTable.entrySet().iterator();

        System.out.println("FlinkBWA performs genomic alignment using bwa in a Hadoop/YARN cluster");
        System.out.println(" usage: " + this.headerAlt);

        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();

            String groupName = (String) pair.getKey();
            if (groupName.contains("algorithm")) {
                System.out.println("BWA algorithm options: ");
            } else if (groupName.contains("sort")) {
                System.out.println("Sorting options: ");
            } else if (groupName.contains("help")) {
                System.out.println("Help options: ");
            } else if (groupName.contains("reads")) {
                System.out.println("Input FASTQ reads options: ");
            } else if (groupName.contains("Number of partitions")) {
                System.out.println("Flink options: ");
            } else if (groupName.contains("Prefix for the index")) {
                System.out.println("Index options: ");
            } else if (groupName.contains("directly to BWA")) {
                System.out.println("BWA arguments options: ");
            } else if (groupName.contains("The program is going to merge")) {
                System.out.println("Reducer options: ");
            } else if (groupName.contains("The tmpDir is used for selecting")) {
                System.out.println("Tmp path options: ");
            } else {
                System.out.println(groupName + "options: ");
            }

            String newOptionOutput;
            ArrayList<Option> availableOptions = ((ArrayList<Option>) pair.getValue());
            for (int i = 0; i < availableOptions.size(); i++) {
                Option currentOption = availableOptions.get(i);
                newOptionOutput = "  " + "-" + currentOption.getOpt() + ", --" + currentOption.getLongOpt();

                if (currentOption.hasArg()) {
                    newOptionOutput = newOptionOutput + " <" + currentOption.getArgName() + ">";
                }

                while (newOptionOutput.length() <= 50) {
                    newOptionOutput = newOptionOutput + " ";
                }

                newOptionOutput = newOptionOutput + currentOption.getDescription();
                System.out.println(newOptionOutput);
            }
            System.out.println("");
        }
        System.out.println(this.footer);
    }

    /**
     * Constructor to use with no options
     */
    public BwaOptions() {
    }

    /**
     * Constructor to use from within FlinkBWA from the Linux console
     *
     * @param args The arguments that the user is going to provide from the Linux console
     */
    public BwaOptions(String[] args) {
        //Parse arguments
        for (String argument : args) {
            LOG.info("[" + this.getClass().getName() + "] :: Received argument: " + argument);
        }
        //Algorithm options
        this.options = this.initOptions();
        //To print the help
        HelpFormatter formatter = new HelpFormatter();

        //Parse the given arguments
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd;

        try {
            cmd = parser.parse(this.options, args);
            //We look for the algorithm
            if (cmd.hasOption('m') || cmd.hasOption("mem")) {
                //Case of the mem algorithm
                memAlgorithm = true;
                alnAlgorithm = false;
                bwaswAlgorithm = false;
            } else if (cmd.hasOption('a') || cmd.hasOption("aln")) {
                // Case of aln algorithm
                alnAlgorithm = true;
                memAlgorithm = false;
                bwaswAlgorithm = false;
            } else if (cmd.hasOption('b') || cmd.hasOption("bwasw")) {
                // Case of bwasw algorithm
                bwaswAlgorithm = true;
                memAlgorithm = false;
                alnAlgorithm = false;
            } else {
                // Default case. Mem algorithm
                LOG.warn("[" + this.getClass().getName() + "] :: The algorithm "
                        + cmd.getOptionValue("algorithm")
                        + " could not be found\nSetting to default mem algorithm\n");
                memAlgorithm = true;
                alnAlgorithm = false;
                bwaswAlgorithm = false;
            }
            //We look for the index
            if (cmd.hasOption("index") || cmd.hasOption('i')) {
                indexPath = cmd.getOptionValue("index");
            }

            //Partition number
            if (cmd.hasOption("partitions") || cmd.hasOption('n')) {
                partitionNumber = Integer.parseInt(cmd.getOptionValue("partitions"));
            }

            // BWA arguments
            if (cmd.hasOption("bwa") || cmd.hasOption('w')) {
                bwaArgs = cmd.getOptionValue("bwa");
            }

            // Paired or single reads
            if (cmd.hasOption("paired") || cmd.hasOption('p')) {
                pairedReads = true;
                singleReads = false;
            } else if (cmd.hasOption("single") || cmd.hasOption('s')) {
                pairedReads = false;
                singleReads = true;
            } else {
                LOG.warn("[" + this.getClass().getName() + "] :: Reads argument could not be found\nSetting it to default paired reads\n");
                pairedReads = true;
                singleReads = false;
            }

            // Sorting
            if (cmd.hasOption('f') || cmd.hasOption("hdfs")) {
                this.sortFastqReadsHdfs = true;
                this.sortFastqReads = false;
            } else if (cmd.hasOption('k') || cmd.hasOption("flink")) {
                this.sortFastqReadsHdfs = false;
                this.sortFastqReads = true;
            } else {
                this.sortFastqReadsHdfs = false;
                this.sortFastqReads = false;
            }

            // Use reducer
            if (cmd.hasOption('r') || cmd.hasOption("reducer")) {
                this.useReducer = true;
            }

            // Specify alternate tmp dir
            if (cmd.hasOption('t') || cmd.hasOption("tmpDir")) {
                this.tmpPath = cmd.getOptionValue("tmpDir");
            }

            // Help
            if (cmd.hasOption('h') || cmd.hasOption("help")) {
                this.printHelp();
                System.exit(0);
            }

            //Input and output paths
            String otherArguments[] = cmd.getArgs(); //With this we get the rest of the arguments

            if ((otherArguments.length != 2) && (otherArguments.length != 3)) {
                LOG.error("[" + this.getClass().getName() + "] No input and output has been found. Aborting.");
                for (String tmpString : otherArguments) {
                    LOG.error("[" + this.getClass().getName() + "] Other args:: " + tmpString);
                }
                System.exit(1);
            } else if (otherArguments.length == 2) {
                inputPath = otherArguments[0];
                outputPath = otherArguments[1];
            } else if (otherArguments.length == 3) {
                inputPath = otherArguments[0];
                inputPath2 = otherArguments[1];
                outputPath = otherArguments[2];
            }
            outputHdfsDir = outputPath;

        } catch (UnrecognizedOptionException e) {
            e.printStackTrace();
            //formatter.printHelp(correctUse, header, options, footer, true);
            this.printHelp();
            System.exit(1);
        } catch (MissingOptionException e) {
            //formatter.printHelp(correctUse, header, options, footer, true);
            this.printHelp();
            System.exit(1);
        } catch (ParseException e) {
            //formatter.printHelp( correctUse,header, options,footer , true);
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Function to init the FlinkBWA available options
     *
     * @return An Options object containing the available options
     */
    public Options initOptions() {
        Options privateOptions = new Options();
        //Algorithm options
        OptionGroup algorithm = new OptionGroup();
        Option mem = new Option("m", "mem", false, "The MEM algorithm will be used");
        algorithm.addOption(mem);

        Option aln = new Option("a", "aln", false, "The ALN algorithm will be used");
        algorithm.addOption(aln);

        Option bwasw = new Option("b", "bwasw", false, "The bwasw algorithm will be used");
        algorithm.addOption(bwasw);

        privateOptions.addOptionGroup(algorithm);

        //Paired or single reads
        OptionGroup reads = new OptionGroup();

        Option paired = new Option("p", "paired", false, "Paired reads will be used as input FASTQ reads");
        reads.addOption(paired);

        Option single = new Option("s", "single", false, "Single reads will be used as input FASTQ reads");
        reads.addOption(single);

        privateOptions.addOptionGroup(reads);

        // Options to BWA
        OptionGroup bwaOptionsGroup = new OptionGroup();
        Option bwaArgs = new Option("w", "bwa", true, "Arguments passed directly to BWA");
        bwaArgs.setArgName("\"BWA arguments\"");

        bwaOptionsGroup.addOption(bwaArgs);

        privateOptions.addOptionGroup(bwaOptionsGroup);

        //Index
        OptionGroup indexGroup = new OptionGroup();
        Option index = new Option("i", "index", true, "Prefix for the index created by bwa to use - setIndexPath(string)");
        index.setArgName("Index prefix");
        index.setRequired(true);

        indexGroup.addOption(index);

        privateOptions.addOptionGroup(indexGroup);

        //Partition number
        OptionGroup flinkGroup = new OptionGroup();
        Option partitions = new Option("n", "partitions", true,
                "Number of partitions to divide input - setPartitionNumber(int)");
        partitions.setArgName("Number of partitions");

        flinkGroup.addOption(partitions);

        privateOptions.addOptionGroup(flinkGroup);


        OptionGroup reducerGroup = new OptionGroup();
        Option reducer = new Option("r", "reducer", false, "The program is going to merge all the final results in a reducer phase");

        reducerGroup.addOption(reducer);

        privateOptions.addOptionGroup(reducerGroup);

        //Sorting
        OptionGroup sorting = new OptionGroup();

        Option hdfs = new Option("f", "hdfs", false, "The HDFS is used to perform the input FASTQ reads sort");
        sorting.addOption(hdfs);

        Option flink = new Option("k", "flink", false, "the Flink engine is used to perform the input FASTQ reads sort");
        sorting.addOption(flink);

        privateOptions.addOptionGroup(sorting);

        // Tmp Dir
        OptionGroup tmpPath = new OptionGroup();
        Option tmpDir = new Option("t", "tmpDir", true, "The tmpDir is used for selecting the temp dir used for temporal storage");
        tmpDir.setArgName("Temporary Directory");
        tmpPath.addOption(tmpDir);
        privateOptions.addOptionGroup(tmpPath);

        // Help
        OptionGroup helpGroup = new OptionGroup();
        Option help = new Option("h", "help", false, "Shows this help");

        helpGroup.addOption(help);

        privateOptions.addOptionGroup(helpGroup);

        return privateOptions;
    }

    /**
     * Getter for BWA args
     *
     * @return A String containing additional BWA arguments
     */
    public String getBwaArgs() {
        return bwaArgs;
    }

    /**
     * Setter for BWA additional arguments
     *
     * @param bwaArgs
     */
    public void setBwaArgs(String bwaArgs) {
        this.bwaArgs = bwaArgs;
    }

    /**
     * Getter for the output directory in HDFS
     *
     * @return A string containing the output directory in HDFS
     */
    public String getOutputHdfsDir() {
        return outputHdfsDir;
    }

    /**
     * Setter for the output directory in HDFS
     *
     * @param outputHdfsDir String containing the path in HDFS where results are going to be stored
     */
    public void setOutputHdfsDir(String outputHdfsDir) {
        this.outputHdfsDir = outputHdfsDir;
    }

    /**
     * Getter for the option of the "mem" algorithm
     *
     * @return A boolean value that is true if the "mem" algorithm is going to be used
     */
    public boolean isMemAlgorithm() {
        return memAlgorithm;
    }

    /**
     * Setter for the option of the "mem" algorithm
     *
     * @param memAlgorithm A boolean value that is true if the "mem" algorithm is going to be used
     */
    public void setMemAlgorithm(boolean memAlgorithm) {
        this.memAlgorithm = memAlgorithm;

        if (memAlgorithm) {
            this.setAlnAlgorithm(false);
            this.setBwaswAlgorithm(false);
        }
    }

    /**
     * Getter for the option of the "aln" algorithm
     *
     * @return A boolean value that is true if the "aln" algorithm is going to be used
     */
    public boolean isAlnAlgorithm() {
        return alnAlgorithm;
    }

    /**
     * Setter for the option of the "aln" algorithm
     *
     * @param alnAlgorithm A boolean value that is true if the "aln" algorithm is going to be used
     */
    public void setAlnAlgorithm(boolean alnAlgorithm) {
        this.alnAlgorithm = alnAlgorithm;

        if (alnAlgorithm) {
            this.setMemAlgorithm(false);
            this.setBwaswAlgorithm(false);
        }
    }

    /**
     * Getter for the option of the "bwasw" algorithm
     *
     * @return A boolean value that is true if the "bwasw" algorithm is going to be used
     */
    public boolean isBwaswAlgorithm() {
        return bwaswAlgorithm;
    }

    /**
     * Setter for the option of the "bwasw" algorithm
     *
     * @param bwaswAlgorithm A boolean value that is true if the "bwasw" algorithm is going to be used
     */
    public void setBwaswAlgorithm(boolean bwaswAlgorithm) {
        this.bwaswAlgorithm = bwaswAlgorithm;

        if (bwaswAlgorithm) {
            this.setAlnAlgorithm(false);
            this.setMemAlgorithm(false);
        }
    }

    /**
     * Getter to know if the paired reads are going to be used
     *
     * @return A boolean value that indicates if paired reads are used
     */
    public boolean isPairedReads() {
        return pairedReads;
    }

    /**
     * Setter for the option of paired reads
     *
     * @param pairedReads Boolean value that indicates if the paired reads are going to be used or not
     */
    public void setPairedReads(boolean pairedReads) {
        this.pairedReads = pairedReads;
    }

    /**
     * Getter to know if the single reads are going to be used
     *
     * @return A boolean value that indicates if single reads are used
     */
    public boolean isSingleReads() {
        return singleReads;
    }

    /**
     * Setter for the option of single reads
     *
     * @param singleReads Boolean value that indicates if the single reads are going to be used or not
     */
    public void setSingleReads(boolean singleReads) {
        this.singleReads = singleReads;
    }

    /**
     * Getter for the index path
     *
     * @return A String containing the path for the index
     */
    public String getIndexPath() {
        return indexPath;
    }

    /**
     * Setter for the index path
     *
     * @param indexPath A String containing the path where the index is stored
     */
    public void setIndexPath(String indexPath) {
        this.indexPath = indexPath;
    }

    /**
     * Getter for the first of the FASTQ files
     *
     * @return A String with the path of the first of the FASTQ files
     */
    public String getInputPath() {
        return inputPath;
    }

    /**
     * Setter for the first of the FASTQ files
     *
     * @param inputPath A String containing the path of the first of the FASTQ files
     */
    public void setInputPath(String inputPath) {
        this.inputPath = inputPath;
    }

    /**
     * Getter for the output path
     *
     * @return A String containing the location where the output files are going to be stored
     */
    public String getOutputPath() {
        return outputPath;
    }

    /**
     * Setter for the output path
     *
     * @param outputPath A String containing the location where the output files are going to be
     *                   stored
     */
    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }

    /**
     * Getter for the number of partitions to use
     *
     * @return An integer that represents the number of partitions to use
     */
    public int getPartitionNumber() {
        return partitionNumber;
    }

    /**
     * Setter for the number of partitions to use
     *
     * @param partitionNumber An integer that represents the number of partitions to use
     */
    public void setPartitionNumber(int partitionNumber) {
        this.partitionNumber = partitionNumber;
    }

    /**
     * Getter for the second of the FASTQ files
     *
     * @return A String with the path of the second of the FASTQ files
     */
    public String getInputPath2() {
        return inputPath2;
    }

    /**
     * Setter for the second of the FASTQ files
     *
     * @param inputPath2 A String with the path of the second of the FASTQ files
     */
    public void setInputPath2(String inputPath2) {
        this.inputPath2 = inputPath2;
    }

    /**
     * Getter for the option to sort the input reads using Flink
     *
     * @return A boolean value that represents if the sorting method using Flink is going to be used
     */
    public boolean isSortFastqReads() {
        return sortFastqReads;
    }

    /**
     * Setter for the option to sort the input reads using Flink
     *
     * @param sortFastqReads A boolean value that represents if the sorting method using Flink is
     *                       going to be used
     */
    public void setSortFastqReads(boolean sortFastqReads) {
        this.sortFastqReads = sortFastqReads;
    }

    /**
     * Getter for the option to sort the input reads using HDFS
     *
     * @return A boolean value that represents if the sorting method using HDFS is going to be used
     */
    public boolean isSortFastqReadsHdfs() {
        return sortFastqReadsHdfs;
    }

    /**
     * Setter for the option to sort the input reads using HDFS
     *
     * @param sortFastqReadsHdfs A boolean value that represents if the sorting method using HDFS is
     *                           going to be used
     */
    public void setSortFastqReadsHdfs(boolean sortFastqReadsHdfs) {
        this.sortFastqReadsHdfs = sortFastqReadsHdfs;
    }

    /**
     * Getter for the option of using a final reducer
     *
     * @return A boolean value that indicates if the program is going to use a final reducer function or not
     */
    public boolean getUseReducer() {
        return this.useReducer;
    }

    /**
     * Setter for the option of using a final reducer
     *
     * @param newValueReducer The new value for the use reducer variable
     */
    public void setuseReducer(boolean newValueReducer) {
        this.useReducer = newValueReducer;
    }

    /**
     * Getter for the option of using an alternate tmp dir
     *
     * @return A string containing the alternative tmp dir if present
     */
    public String getTmpDirectory() {
        return this.tmpPath;
    }

    /**
     * Setter for the option of using an alternative tmp dir
     *
     * @param tmpDir The new value for the tmp dir
     */
    public void setTmpDirectory(String tmpDir) {
        this.tmpPath = tmpDir;
    }
}
