package org.jgi.spark.localcluster.rdd

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.jgi.spark.localcluster.myredis.JedisManagerSingleton
import org.jgi.spark.localcluster.{DNASeq, JavaUtils}
import redis.clients.util.SafeEncoder

import scala.collection.JavaConversions._

/**
  * Created by Lizhen Shi on 5/23/17.
  */
class RedisKmerCountRDD
(sc: SparkContext, val hostsAndPorts: Array[(String, Int)], val n_slot_per_ins: Int)
  extends RDD[(DNASeq, Int)](sc, Seq.empty) with LazyLogging {

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    Seq(split.asInstanceOf[RedisPartition].ip)
  }


  override protected def getPartitions: Array[Partition] = {
    hostsAndPorts.map {
      case (ip, port) =>
        0.until(n_slot_per_ins).map(j => (ip, port, j))
    }.flatten.zipWithIndex.map {
      case ((ip, port, slot), idx) =>
        new RedisPartition(idx, ip, port, slot)
    }

  }

  override def compute(split: Partition, context: TaskContext): Iterator[(DNASeq, Int)] = {
    val partition: RedisPartition = split.asInstanceOf[RedisPartition]
    val ips = JavaUtils.getAllIPs.map(_.getHostAddress)
    if (!ips.contains(partition.ip)) {
      logger.info(s"partation ip ${partition.ip} is not the node ip ${ips.mkString(",")}")
    }
    val jedis = JedisManagerSingleton.instance(hostsAndPorts).getJedis(partition.ip, partition.port)

    val resp = jedis.hgetAll(SafeEncoder.encode(partition.slot.toString))

    val result = resp.map(x => (new DNASeq(x._1), SafeEncoder.encode(x._2).toInt))

    jedis.close()
    result.toIterator
  }
}