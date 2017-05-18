#!/bin/bash
spark-submit --master local[12] --driver-memory $1\
         --conf spark.network.timeout=360000\
         --conf spark.eventLog.enabled=true\
         --conf spark.eventLog.dir=$SCRATCH/spark/spark_event_logs\
         /path to/target/scala-2.10/localcluster_2.10-0.1.jar config.txt
