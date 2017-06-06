INPUT=tmp/5G_edges.txt
OUTPUT=tmp/5G_cc.txt
WAIT=1
ENABLE_LOG=true
TARGET=target/scala-2.11/LocalCluster-assembly-0.1.jar
 
nohup /home/spark/software/spark/bin/spark-submit --master spark://genomics-ecs1:7077 --deploy-mode client --driver-memory 55G --driver-cores 5 --executor-memory 20G  --executor-cores 2 --conf spark.default.parallelism=54 --conf spark.driver.maxResultSize=5g --conf spark.network.timeout=360000 --conf spark.eventLog.enabled=$ENABLE_LOG $TARGET \
GraphCC --wait $WAIT  -i $INPUT  -o $OUTPUT -n 81 &
