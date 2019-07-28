# FlinkBWA

Repositorio del Trabajo de Fin de Grado (TFG): FlinkBWA, uso de tecnologías Big Data para el alineamiento de secuencias genéticas.

--

## What's it?

FlinkBWA is a tool that integrates the Burrows-Wheeler Aligner--BWA on a Apache Flink framework running on the top of Hadoop. The current version of FlinkBWA (v0.1, June 2019) supports the following BWA algorithms:

- BWA-MEM
- BWA-backtrack
- BWA-SW

All of them work with single-reads and paired-end reads.

## Structure

The project keeps a standard Maven structure. The source code is in the src/main folder. Inside it, we can find two subfolders:

+ java: here is where the Java code is stored.
+ native: here the BWA native code (C) and the glue logic for JNI is stored.

## Requirements

Requirements to build FlinkBWA are the same than the ones to build BWA, with this exceptions: 
- JAVA_HOME environment variable should be defined. If not, you can define it in the /src/main/native/Makefile.common file.
- Include the flag -fPIC in the Makefile of the considered BWA version. To do this, the user just need to add this option to the end of the CFLAGS variable in the BWA Makefile.
- Maven (>v3.1.1)
- Hadoop cluster

## Building

```
git clone https://github.com/silviaralcaraz/FlinkBWA.git
cd FlinkBWA
mvn clean package
```

This will create the target folder, which will contain the jar file needed to run FlinkBWA:
- **FlinkBWA-1.0-SNAPSHOT.jar**: jar file to launch with Flink.

## Running FlinkBWA

Here it is an example of how to execute FlinkBWA using the BWA-MEM algorithm with single-end reads. The example assumes that our index is stored in all the cluster nodes at /Data/HumanBase/. The index can be obtained from BWA using "bwa index".

1. First, we get the input FASTQ read from the 1000 Genomes Project ftp:

```
wget ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/phase3/data/NA12750/sequence_read/ERR000589_1.filt.fastq.gz
```

2. Next, the downloaded file should be uncompressed:

```
gzip -d ERR000589_1.filt.fastq.gz
```

3. File should be uploaded to HDFS:

```
hdfs dfs -copyFromLocal ERR000589_1.filt.fastq ERR000589_1.filt.fastq
```

4. Finally, we can execute FlinkBWA on the cluster. Again, we assume that Flink is stored at flink-1.7.2:

```
flink-1.7.2$ ./bin/flink run -c com.github.flinkbwa.FlinkBWA -m yarn-cluster -yn 4 -ytm 30g FlinkBWA-1.0-SNAPSHOT.jar -m -s -n 4 --index /Data/GenomaHumano/GCA_000001405.28_GRCh38.p13_genomic.fna /user/user_name/ERR000589_1.filt.fastq Output_ERR000589
```

Options used:

+ -m: Sequence alignment algorithm.
+ -s: Use single-end reads.
+ --index index_prefix: index prefix is specified. The index must be available in all the cluster nodes at the same location. 
+ The last three arguments are the input and output HDFS files.

After the execution, in order to move the output to the local filesystem use:

```
hdfs dfs -copyToLocal Output_ERR000589/* ./
```

In case of not using a reducer, the output will be split into several pieces (files). If we want to put it together we can use "samtools merge".

If you want to check all the available options, execute the command:

```
 ./bin/flink run -c com.github.flinkbwa.FlinkBWA -m yarn - cluster FlinkBWA-1.0-SNAPSHOT.jar -h
```
