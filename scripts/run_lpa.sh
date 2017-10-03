. `dirname $0`/config
INPUT=tmp/${PREFIX}_edges.txt_$K
OUTPUT=tmp/${PREFIX}_lpa.txt_$K
WAIT=1

#min_shared_kmers=20

#PL=`expr $PL \* 5`
CMD=`cat<<EOF
$SPARK_SUBMIT --master $MASTER --deploy-mode client --driver-memory 55G --driver-cores 5 --executor-memory 18G  --executor-cores 2 --conf spark.default.parallelism=$PL --conf spark.driver.maxResultSize=5g --conf spark.network.timeout=360000 --conf spark.eventLog.enabled=$ENABLE_LOG --conf spark.speculation=true --class org.jgi.spark.localcluster.tools.GraphLPA2  $TARGET \
--wait $WAIT  -i $INPUT  -o $OUTPUT --min_shared_kmers $min_shared_kmers  --max_shared_kmers $max_shared_kmers --min_reads_per_cluster $min_reads_per_cluster --max_iteration 50 
EOF`

echo $CMD

if [ $# -gt 0 ]
  then
     nohup $CMD 
     echo "submitted"
else
     echo "dry-run, not runing"
fi

