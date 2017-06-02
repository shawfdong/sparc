package org.jgi.spark.localcluster.rdd

import org.apache.spark.Partition
import org.jgi.spark.localcluster.myredis.RedisSlot

/**
  * Created by Lizhen Shi on 5/23/17.
  */

class RedisPartition(val index: Int, val ip: String, port: Int, slot_idx: Int) extends Partition {
  val slot = RedisSlot(ip, port, slot_idx)

  def key(k: String): String = slot.key(k)
}
