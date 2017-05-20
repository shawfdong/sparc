nohup /home/spark/software/spark/bin/spark-submit --class org.jgi.spark.localcluster.tools.GraphCC --master spark://genomics-ecs1:7077 --deploy-mode client --driver-memory 30G --driver-cores 5 --executor-memory 16G --num-executors 6 --executor-cores 2 --conf spark.default.parallelism=100 target/scala-2.11/LocalCluster-assembly-0.1.jar --wait 1000 \
-i 1G_edges.txt -o 1G_cc.txt &
