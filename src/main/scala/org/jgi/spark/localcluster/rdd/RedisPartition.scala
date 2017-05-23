package org.jgi.spark.localcluster.rdd

import org.apache.spark.Partition

/**
  * Created by Lizhen Shi on 5/23/17.
  */

case class RedisPartition(index: Int, ip:String,port:Int, slot:Int) extends Partition
