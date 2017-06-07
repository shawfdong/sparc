INPUT=tmp/5G_kmerreads.txt
OUTPUT=tmp/5G_edges.txt
WAIT=1
TARGET=target/scala-2.11/LocalCluster-assembly-0.1.jar 

nohup /home/spark/software/spark/bin/spark-submit --master spark://genomics-ecs1:7077 --deploy-mode client --driver-memory 55G --driver-cores 5 --executor-memory 20G --executor-cores 2 --conf spark.executor.extraClassPath=$TARGET --conf spark.driver.maxResultSize=5g --conf spark.network.timeout=360000  --conf spark.default.parallelism=2700  $TARGET \
GraphGen --wait $WAIT -i $INPUT  -k 31 -o $OUTPUT  &
