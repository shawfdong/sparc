INPUT=data/1G.seq
OUTPUT=tmp/11G_kc_seq
REDIS=192.168.0.12:20000,192.168.0.12:20001,192.168.0.12:20002,192.168.0.12:20003,192.168.0.13:20000,192.168.0.13:20001,192.168.0.13:20002,192.168.0.13:20003

#nohup /home/spark/software/spark/bin/spark-submit --class org.jgi.spark.localcluster.tools.KmerCounting --master spark://genomics-ecs1:7077 --deploy-mode client --driver-memory 30G --driver-cores 5 --executor-memory 20G  --executor-cores 2 --conf spark.default.parallelism=100 target/scala-2.11/LocalCluster-assembly-0.1.jar -i $INPUT  -o $OUTPUT  --format seq --contamination 0.00005 --n_partition 32 &
nohup /home/spark/software/spark/bin/spark-submit --master spark://genomics-ecs1:7077 --deploy-mode client --driver-memory 30G --driver-cores 5 --executor-memory 10G  --executor-cores 2 --conf spark.default.parallelism=100 target/scala-2.11/LocalCluster-assembly-0.1.jar \
KmerCounting -i $INPUT  -o $OUTPUT  --format seq --contamination 0.00005 --redis=$REDIS --use_bloom_filter &
