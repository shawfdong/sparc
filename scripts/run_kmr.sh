. `dirname $0`/config
INPUT_KMER=tmp/${PREFIX}_kc_seq
OUTPUT=tmp/${PREFIX}_kmerreads.txt
WAIT=1
OPTS="-C --without_bloomfilter"

PL=`expr $PL \* 5 `

CMD=`cat<<EOF
$SPARK_SUBMIT --master $MASTER --deploy-mode client --driver-memory 55G --driver-cores 5 --executor-memory 18G --executor-cores 2 --conf spark.executor.extraClassPath=$TARGET  --conf spark.driver.maxResultSize=8g --conf spark.network.timeout=360000 --conf spark.default.parallelism=$PL --conf spark.eventLog.enabled=$ENABLE_LOG --conf spark.executor.userClassPathFirst=true --conf spark.driver.userClassPathFirst=true $TARGET  \
KmerMapReads --wait $WAIT --reads $INPUT  --format seq  -o $OUTPUT  -k $K --kmer $INPUT_KMER --contamination $CL --min_kmer_count $min_kmer_count --max_kmer_count $max_kmer_count $OPTS 
EOF`

echo $CMD

if [ $# -gt 0 ]
  then
     nohup $CMD &
     echo "submitted"
else
     echo "dry-run, not runing"
fi

