. `dirname $0`/config
INPUT=tmp/${PREFIX}_edges.txt
OUTPUT=tmp/${PREFIX}_cc.txt
WAIT=1
 
CMD=`cat<<EOF
$SPARK_SUBMIT --master $MASTER --deploy-mode client --driver-memory 55G --driver-cores 5 --executor-memory 20G  --executor-cores 2 --conf spark.default.parallelism=54 --conf spark.driver.maxResultSize=5g --conf spark.network.timeout=360000 --conf spark.eventLog.enabled=$ENABLE_LOG $TARGET \
GraphCC --wait $WAIT  -i $INPUT  -o $OUTPUT -n 162 --min_shared_kmers $min_shared_kmers  --max_shared_kmers $max_shared_kmers --min_reads_per_cluster $min_reads_per_cluster --n_iteration 1 --use_graphframes
EOF`

echo $CMD

if [ $# -gt 0 ]
  then
     nohup $CMD &
     echo "submitted"
else
     echo "dry-run, not runing"
fi

