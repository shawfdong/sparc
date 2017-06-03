package org.jgi.spark.localcluster.rdd

import org.apache.spark.Partition
import org.jgi.spark.localcluster.kvstore.KVStoreSlot

/**
  * Created by Lizhen Shi on 6/3/17.
  */

class KVStorePartition(val index: Int, val ip: String, port: Int ) extends Partition {
  val slot = KVStoreSlot(ip, port )

  def key(k: String): String = slot.key(k)
}
