package org.jgi.spark.localcluster.rdd

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.jgi.spark.localcluster.myredis.{JedisManagerSingleton, RedisSlot}
import org.jgi.spark.localcluster.{Constant, DNASeq, JavaUtils}
import redis.clients.util.SafeEncoder

import scala.collection.JavaConversions._

/**
  * Created by Lizhen Shi on 5/23/17.
  */
class RedisKmerCountRDD
(sc: SparkContext, val redisSlots: Array[RedisSlot])
  extends RDD[(DNASeq, Int)](sc, Seq.empty) with LazyLogging {

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
        val key = partition.key(Constant.KMER_COUNTING_REDIS_HASH_KEY)
        val n = jedis.hlen(key)
        jedis.close()
        n.toInt  //currently max key number is 2^32
    }
      counts.sum
  }

  override def compute(split: Partition, context: TaskContext): Iterator[(DNASeq, Int)] = {
    val partition: RedisPartition = split.asInstanceOf[RedisPartition]
    val ips = JavaUtils.getAllIPs.map(_.getHostAddress)
    if (!ips.contains(partition.ip)) {
      logger.info(s"partation ip ${partition.ip} is not the node ip ${ips.mkString(",")}")
    }
    val jedis = JedisManagerSingleton.instance(redisSlots).getJedis(partition.slot)

    val resp = jedis.hgetAll(SafeEncoder.encode(partition.key(Constant.KMER_COUNTING_REDIS_HASH_KEY)))

    val result = resp.map(x => (new DNASeq(x._1), SafeEncoder.encode(x._2).toInt))

    jedis.close()
    result.toIterator
  }
}