. `dirname $0`/config
INPUT=tmp/${PREFIX}_lpa.txt_$K
READS=data/${PREFIX}.seq 
OUTPUT=tmp/${PREFIX}_lpaseq.txt_$K
WAIT=1
 
CMD=`cat<<EOF
$SPARK_SUBMIT --master $MASTER --deploy-mode client --driver-memory 55G --driver-cores 5 --executor-memory 20G  --executor-cores 2 --conf spark.default.parallelism=54 --conf spark.driver.maxResultSize=5g --conf spark.network.timeout=360000 --conf spark.speculation=true --conf spark.eventLog.enabled=$ENABLE_LOG $TARGET \
CCAddSeq --wait $WAIT  -i $INPUT --reads $READS  -o $OUTPUT 
EOF`

echo $CMD

if [ $# -gt 0 ]
  then
     nohup $CMD 
     echo "submitted"
else
     echo "dry-run, not runing"
fi

