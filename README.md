# Local-Cluster: cluster reads based on shared kmers

## Requirements:

The development environment uses the following settings (see details in build.sbt)

1. Spark 2.0.1
2. Scala 2.11.8
3. Hadoop 2.7.3
4. JAVA 1.8

To run in your environment, change the versions of dependencies correspondingly and run test to check if it works.

## Build (Scala):

0. install java, sbt, spark and hadoop if necessary, and make sure environment variables of JAVA_HOME, HADOOP_HOME, SPARK_HOME are set.
1. optionally use "sbt -mem 4096 test" to run unit tests (this example uses 4G memory).
2. run "sbt assembly" to make the assembly jar.
    
## Input Format

### Seq

A seq file contains lines of ID, HEAD, SEQ which are separated by TAB ("\t"). An example is

    1	k103_14993672 flag=1 multi=14.0000 len=10505	TCTAAGTAACATTGACACGTAGCACACATAG
    2	k103_13439317 flag=0 multi=59.2840 len=4715	CGCAGTTATCTTTTTCAAATTCTGGCCGATAA

Fasta or fastq files can be converted to .seq file using the provided scripts:

    fastaToSeq.py
    fastqToSeq.py
    
for contigs files that have been sorted, please enable "shuffle" function to balance data partitions
for fasta files with sequence span multiple lines (formatted), using the following command:

    awk '!/^>/ { printf "%s", $0; n = "\n" } /^>/ { print n $0; n = "" } END { printf "%s", n }' multi.fa > singleline.fa

For seq file of old version, the numeric ID may be missing, use the script to add IDs, e.g.

     cat *.seq | scripts/seq_add_id.py > new_seq.txt

### Parquet

_Parquet_ file is a binary compressed format file. It can be converted from _Seq_ file by

    spark-submit --master local[4] --driver-memory 16G --class org.jgi.spark.localcluster.tools.Seq2Parquet target/scala-2.11/LocalCluster-assembly-0.1.jar -i test/small/*.seq -o tmp/parquet_output

Help

    $ spark-submit --master local[4] --driver-memory 16G --class org.jgi.spark.localcluster.tools.Seq2Parquet target/scala-2.11/LocalCluster-assembly-*.jar --help

    Seq2Parquet 0.1
    Usage: Seq2Parquet [options]

      -i, --input <dir>        a local dir where seq files are located in,  or a local file, or an hdfs file
      -p, --pattern <pattern>  if input is a local dir, specify file patterns here. e.g. *.seq, 12??.seq
      -o, --output <dir>       output of the top k-mers
      --coalesce               coalesce the output
      -n, --n_partition <value>
                               paritions for the input, only applicable to local files
      --help                   prints this usage text

### Base64

Although Parquet has advantages of data schema and compression etc, use base64 encoding is simpler. Its text format is more flexible and the compression is better.
Convert seq file to base64 format use

    spark-submit --master local[4] --driver-memory 16G --class org.jgi.spark.localcluster.tools.Seq2Base64 target/scala-2.11/LocalCluster-assembly-0.1.jar -i test/small/*.seq -o tmp/base64_output --coalesce

Help

     $ spark-submit --master local[4] --driver-memory 16G --class org.jgi.spark.localcluster.tools.Seq2Base64 target/scala-2.11/LocalCluster-assembly-0.1.jar --help

    Seq2Base64 0.1
    Usage: Seq2Base64 [options]

      -i, --input <dir>        a local dir where seq files are located in,  or a local file, or an hdfs file
      -p, --pattern <pattern>  if input is a local dir, specify file patterns here. e.g. *.seq, 12??.seq
      -o, --output <dir>       output of the top k-mers
      --coalesce               coalesce the output
      -n, --n_partition <value>
                               paritions for the input, only applicable to local files
      --help                   prints this usage text

## Kmer counting and take tops
Having data on hand (local or hdfs), next step is to counting kmers and only keep the tops.
Example for seq files

    spark-submit --master local[4] --driver-memory 16G --class org.jgi.spark.localcluster.tools.KmerCounting target/scala-2.11/LocalCluster-assembly-0.1.jar -i test/small/*.seq -o tmp/seq_result.txt --format seq

and example for base64 files

    spark-submit --master local[4] --driver-memory 16G --class org.jgi.spark.localcluster.tools.KmerCounting target/scala-2.11/LocalCluster-assembly-0.1.jar -i test/small/*.base64 -o tmp/base64_result.txt --format base64

Also use "--help" to see the usage

    kmer counting 0.1
    Usage: KmerCounting [options]

      -i, --input <dir>        a local dir where seq files are located in,  or a local file, or an hdfs file
      -p, --pattern <pattern>  if input is a local dir, specify file patterns here. e.g. *.seq, 12??.seq
      -o, --output <dir>       output of the top k-mers
      --format <format>        input format (seq, parquet or base64)
      -n, --n_partition <value>
                               paritions for the input, only applicable to local files
      -k, --kmer_length <value>
                               length of k-mer
      -n, --n_iteration <value>
                               #iterations to finish the task. default 1. set a bigger value if resource is low.
      -c, --contamination <value>
                               the fraction of top k-mers to keep, others are removed likely due to contamination
      --scratch_dir <dir>      where the intermediate results are
      --help                   prints this usage text


## Find the shared reads for one of top kmers
Seq example

    spark-submit --master local[4] --driver-memory 16G --class org.jgi.spark.localcluster.tools.KmerMapReads target/scala-2.11/LocalCluster-assembly-0.1.jar --reads test/small/sample.seq --format seq -k 31 --kmer test/kmercounting_test.txt  --output test/kmer_reads.txt

Base64 example

    spark-submit --master local[4] --driver-memory 16G --class org.jgi.spark.localcluster.tools.KmerMapReads target/scala-2.11/LocalCluster-assembly-0.1.jar --reads test/small/sample.base64 --format base64 -k 31 --kmer test/kmercounting_test.txt  --output test/kmer_reads.txt

Also use "--help" to see the usage

    KmerMapReads 0.1
    Usage: KmerMapReads [options]

      --reads <dir/file>       a local dir where seq files are located in,  or a local file, or an hdfs file
      --kmer <file>            Kmers to be keep. e.g. output from kmer counting
      -p, --pattern <pattern>  if input is a local dir, specify file patterns here. e.g. *.seq, 12??.seq
      -o, --output <dir>       output of the top k-mers
      --format <format>        input format (seq, parquet or base64)
      -n, --n_partition <value>
                               paritions for the input, only applicable to local files
      -k, --kmer_length <value>
                               length of k-mer
      --min_kmer_count <value>
                               minimum number of reads that shares a kmer
      --max_kmer_count <value>
                               maximum number of reads that shares a kmer. greater than max_kmer_count, however don't be too big
      -n, --n_iteration <value>
                               #iterations to finish the task. default 1. set a bigger value if resource is low.
      --scratch_dir <dir>      where the intermediate results are
      --help                   prints this usage text

## Generate graph edges
Example

    spark-submit --master local[4] --driver-memory 16G --class org.jgi.spark.localcluster.tools.GraphGen target/scala-2.11/LocalCluster-assembly-0.1.jar -i test/kmer_reads.txt  -o test/edges.txt -k 31

Also use "--help" to see the usage

    GraphGen 0.1
    Usage: GraphGen [options]

      -i, --kmer_reads <file>  reads that a kmer shares. e.g. output from KmerMapReads
      -o, --output <dir>       output of the top k-mers
      -k, --kmer_length <value>
                               length of k-mer
      --min_shared_kmers <value>
                               minimum number of kmers that two reads share
      -n, --n_iteration <value>
                               #iterations to finish the task. default 1. set a bigger value if resource is low.
      --scratch_dir <dir>      where the intermediate results are
      --help                   prints this usage text


## Find connected components
Example

    spark-submit --master local[4] --driver-memory 16G --class org.jgi.spark.localcluster.tools.GraphCC target/scala-2.11/LocalCluster-assembly-0.1.jar -i test/graph_gen_test.txt -o tmp/cc.txt

Also use "--help" to see the usage

    GraphCC 0.1
    Usage: GraphCC [options]

      -i, --edge_file <file>   files of graph edges. e.g. output from GraphGen
      -o, --output <dir>       output of the top k-mers
      -n, --n_iteration <value>
                               #iterations to finish the task. default 1. set a bigger value if resource is low.
      --min_reads_per_cluster <value>
                               minimum reads per cluster
      --scratch_dir <dir>      where the intermediate results are
      --help                   prints this usage text
