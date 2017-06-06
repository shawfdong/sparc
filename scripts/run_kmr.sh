INPUT=data/5G.seq
INPUT_KMER=tmp/5G_kc_seq
OUTPUT=tmp/5G_kmerreads.txt
TARGET=target/scala-2.11/LocalCluster-assembly-0.1.jar
WAIT=1

nohup /home/spark/software/spark/bin/spark-submit --master spark://genomics-ecs1:7077 --deploy-mode client --driver-memory 55G --driver-cores 5 --executor-memory 20G --executor-cores 2 --conf spark.executor.extraClassPath=$TARGET  --conf spark.driver.maxResultSize=5g --conf spark.network.timeout=360000 --conf spark.default.parallelism=7200 $TARGET  \
KmerMapReads --wait $WAIT --reads $INPUT  --format seq  -o $OUTPUT  -k 31 --kmer $INPUT_KMER  &
