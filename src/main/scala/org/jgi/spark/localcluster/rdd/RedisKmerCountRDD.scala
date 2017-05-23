package org.jgi.spark.localcluster.rdd

import java.util

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.jgi.spark.localcluster.{DNASeq, JedisManager, JedisManagerSingleton}
import redis.clients.jedis.{Jedis, ScanParams}
import redis.clients.util.SafeEncoder
import collection.JavaConverters._
import scala.collection.JavaConversions._

/**
  * Created by Lizhen Shi on 5/23/17.
  */
class RedisKmerCountRDD
(sc: SparkContext, val hostsAndPorts: Array[(String, Int)], val n_slot_per_ins: Int)
  extends RDD[(DNASeq, Int)](sc, Seq.empty) {

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    Seq(split.asInstanceOf[RedisPartition].ip)
  }


  override protected def getPartitions: Array[Partition] = {
    hostsAndPorts.zipWithIndex.map {
      case ((ip, port), i) =>
        new RedisPartition(i, ip, port, n_slot_per_ins)
    }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[(DNASeq, Int)] = {
    val partition: RedisPartition = split.asInstanceOf[RedisPartition]

    val jedis = JedisManagerSingleton.instance(hostsAndPorts).getJedis(partition.ip, partition.port)

    val p = jedis.pipelined()
    val resp = p.hgetAll(SafeEncoder.encode(partition.slot.toString)).get
    val result = resp.map(x => (new DNASeq(x._1), SafeEncoder.encode(x._2).toInt))

    jedis.close()
    result.toIterator
  }
}