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
  RedisKmerBloomFilterCountRDD = {
    new RedisKmerBloomFilterCountRDD(sc,slots)
  }

  //////////////////// edge
  def edgeCountFromRedis(slots:Array[RedisSlot]):
  RedisEdgeCountRDD = {
    new RedisEdgeCountRDD(sc,slots)
  }

  def edgeCountFromRedisWithBloomFilter(slots:Array[RedisSlot]):
  RedisEdgeBloomFilterCountRDD = {
    new RedisEdgeBloomFilterCountRDD(sc,slots)
  }
}

trait RedisFunctions {
  implicit def toRedisContext(sc: SparkContext): RedisContext = new RedisContext(sc)
}
