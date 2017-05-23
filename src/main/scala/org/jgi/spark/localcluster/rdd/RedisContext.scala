package org.jgi.spark.localcluster.rdd

import org.apache.spark.SparkContext

/**
  * Created by Lizhen Shi on 5/23/17.
  */
class RedisContext(@transient val sc: SparkContext) extends Serializable {

  def kmerCountFromRedis(hostsAndPorts: Array[(String, Int)], n_slot_per_ins: Int):
  RedisKmerCountRDD = {
    new RedisKmerCountRDD(sc, hostsAndPorts, n_slot_per_ins)
  }

}

trait RedisFunctions {
  implicit def toRedisContext(sc: SparkContext): RedisContext = new RedisContext(sc)
}
