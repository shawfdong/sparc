. `dirname $0`/config
INPUT=tmp/${PREFIX}_edges.txt
OUTPUT=tmp/${PREFIX}_cc.txt
WAIT=1

top_nodes_ratio=0.05
big_cluster_threshold=0.05

 
#PL=`expr $PL \* 5`
CMD=`cat<<EOF
$SPARK_SUBMIT --master $MASTER --deploy-mode client --driver-memory 55G --driver-cores 5 --executor-memory 18G  --executor-cores 2 --conf spark.default.parallelism=$PL --conf spark.driver.maxResultSize=5g --conf spark.network.timeout=360000 --conf spark.eventLog.enabled=$ENABLE_LOG --conf spark.speculation=true  $TARGET \
GraphCC --wait $WAIT  -i $INPUT  -o $OUTPUT --min_shared_kmers $min_shared_kmers  --max_shared_kmers $max_shared_kmers --min_reads_per_cluster $min_reads_per_cluster --n_iteration 1 --use_graphframes --top_nodes_ratio $top_nodes_ratio --big_cluster_threshold $big_cluster_threshold
EOF`

echo $CMD

if [ $# -gt 0 ]
  then
     nohup $CMD &
     echo "submitted"
else
     echo "dry-run, not runing"
fi

