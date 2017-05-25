package org.jgi.spark.localcluster.rdd

import org.apache.spark.SparkContext
import org.jgi.spark.localcluster.myredis.RedisSlot

import scala.language.implicitConversions

/**
  * Created by Lizhen Shi on 5/23/17.
  */
class RedisContext(@transient val sc: SparkContext) extends Serializable {

  def kmerCountFromRedis(slots:Array[RedisSlot]):
  RedisKmerCountRDD = {
    new RedisKmerCountRDD(sc,slots)
  }

  def kmerCountFromRedisWithBloomFilter(slots:Array[RedisSlot]):
  RedisKmerCountRDD = {
    new RedisKmerBloomFilterCountRDD(sc,slots)
  }
}

trait RedisFunctions {
  implicit def toRedisContext(sc: SparkContext): RedisContext = new RedisContext(sc)
}
