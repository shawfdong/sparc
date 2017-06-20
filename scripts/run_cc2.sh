INPUT=tmp/5G_edges.txt
OUTPUT=tmp/5G_cc.txt
WAIT=1
TARGET=target/scala-2.11/LocalCluster-assembly-0.1.jar

#ref https://github.com/broadinstitute/gatk/issues/1524
OTHEROPTS='--conf spark.executor.extraJavaOptions="-XX:hashCode=0" --conf spark.driver.extraJavaOptions="-XX:hashCode=0"'
 
nohup /home/spark/software/spark/bin/spark-submit --master spark://genomics-ecs1:7077 --deploy-mode client --driver-memory 55G --driver-cores 5 --executor-memory 48G  --executor-cores 1 --conf spark.default.parallelism=54 --conf spark.driver.maxResultSize=5g --conf spark.network.timeout=360000 $OTHEROPTS $TARGET \
GraphCC2 --wait $WAIT  -i $INPUT  -o $OUTPUT --n_iteration=6 --n_thread=8 &
