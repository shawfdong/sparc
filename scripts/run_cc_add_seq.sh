. `dirname $0`/load_config.sh

READS=$INPUT
INPUT=$OUTPUT_PREFIX/{$PREFIX}_cc.txt
OUTPUT=$OUTPUT_PREFIX/${PREFIX}_result.txt
WAIT=1
 
nohup $SPARK_SUBMIT --master $MASTER --deploy-mode client --driver-memory 55G --driver-cores 5 --executor-memory 20G  --executor-cores 2 --conf spark.default.parallelism=54 --conf spark.driver.maxResultSize=5g --conf spark.network.timeout=360000 --conf spark.speculation=true --conf spark.eventLog.enabled=$ENABLE_LOG $TARGET \
CCAddSeq --wait $WAIT  -i $INPUT --reads $READS  -o $OUTPUT 
