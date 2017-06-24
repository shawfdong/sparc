. `dirname $0`/config
INPUT=tmp/${PREFIX}_edges.txt
OUTPUT=tmp/${PREFIX}_cc.txt
WAIT=1
TARGET=target/scala-2.11/LocalCluster-assembly-0.1.jar

#ref https://github.com/broadinstitute/gatk/issues/1524
OTHEROPTS='--conf spark.executor.extraJavaOptions="-XX:hashCode=0" --conf spark.driver.extraJavaOptions="-XX:hashCode=0"'
 
CMD=`cat<<EOF
$SPARK_SUBMIT --master spark://genomics-ecs1:7077 --deploy-mode client --driver-memory 55G --driver-cores 5 --executor-memory 48G  --executor-cores 1 --conf spark.default.parallelism=54 --conf spark.driver.maxResultSize=8g --conf spark.network.timeout=360000 $OTHEROPTS $TARGET \
GraphCC2 --wait $WAIT  -i $INPUT  -o $OUTPUT --n_iteration=9   --min_shared_kmers $min_shared_kmers --max_shared_kmers $max_shared_kmers --min_reads_per_cluster $min_reads_per_cluster
EOF`

echo $CMD

if [ $# -gt 0 ]
  then
     nohup $CMD &
     echo "submitted"
else
     echo "dry-run, not runing"
fi
