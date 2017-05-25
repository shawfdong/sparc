package org.jgi.spark.localcluster.rdd

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.jgi.spark.localcluster.myredis.{JedisManagerSingleton, RedisSlot}
import org.jgi.spark.localcluster.{Constant, JavaUtils, SingleEdge}

import scala.collection.JavaConversions._

/**
  * Created by Lizhen Shi on 5/23/17.
  */
class RedisEdgeCountRDD
(sc: SparkContext, val redisSlots: Array[RedisSlot])
  extends RDD[(SingleEdge, Int)](sc, Seq.empty) with LazyLogging {

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    Seq(split.asInstanceOf[RedisPartition].slot.ip)
  }


  override protected def getPartitions: Array[Partition] = {
    redisSlots.zipWithIndex.map {
      case (slot, idx) =>
        new RedisPartition(idx, slot.ip, slot.port, slot.slot)
    }

  }

  override def count(): Long = {
    val counts = this.partitions.map {
      p =>
        val partition = p.asInstanceOf[RedisPartition]
        val jedis = JedisManagerSingleton.instance(redisSlots).getJedis(partition.slot)
        val key = partition.key(Constant.EDGE_COUNTING_REDIS_HASH_KEY)
        val n = jedis.hlen(key)
        jedis.close()
        n.toInt //currently max key number is 2^32
    }
    counts.sum
  }

  override def compute(split: Partition, context: TaskContext): Iterator[(SingleEdge, Int)] = {
    val partition: RedisPartition = split.asInstanceOf[RedisPartition]
    val ips = JavaUtils.getAllIPs.map(_.getHostAddress)
    if (!ips.contains(partition.ip)) {
      logger.info(s"partation ip ${partition.ip} is not the node ip ${ips.mkString(",")}")
    }
    val jedis = JedisManagerSingleton.instance(redisSlots).getJedis(partition.slot)

    val resp = jedis.hgetAll(partition.key(Constant.EDGE_COUNTING_REDIS_HASH_KEY))

    val result = resp.map(x => (SingleEdge(x._1), x._2.toInt))

    jedis.close()
    result.toIterator
  }
}