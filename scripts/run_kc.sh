INPUT=data/5G.seq
OUTPUT=tmp/5G_kc_seq
WAIT=1
TARGET=target/scala-2.11/LocalCluster-assembly-0.1.jar 

nohup /home/spark/software/spark/bin/spark-submit --master spark://genomics-ecs1:7077 --deploy-mode client --driver-memory 55G --driver-cores 5 --executor-memory 20G  --executor-cores 2   --conf spark.network.timeout=360000 --conf spark.default.parallelism=3600 --conf spark.executor.extraClassPath=$TARGET --conf spark.speculation=false --conf spark.speculation.multiplier=2 --conf spark.eventLog.enabled=true $TARGET \
KmerCounting --wait $WAIT  -i $INPUT  -o $OUTPUT  --format seq --contamination 0.00005  &
