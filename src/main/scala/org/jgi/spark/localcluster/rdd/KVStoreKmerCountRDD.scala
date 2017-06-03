package org.jgi.spark.localcluster.rdd

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.jgi.spark.localcluster.kvstore.{KVStoreManagerSingleton, KVStoreSlot}
import org.jgi.spark.localcluster.{DNASeq, JavaUtils}

import scala.collection.JavaConversions._

/**
  * Created by Lizhen Shi on 6/3/17.
  */
class KVStoreKmerCountRDD
(sc: SparkContext, val redisSlots: Array[KVStoreSlot],val useBloomFilter:Boolean, val minimumCount:Int)
  extends RDD[(DNASeq, Int)](sc, Seq.empty) with LazyLogging {

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    Seq(split.asInstanceOf[KVStorePartition].slot.ip)
  }


  override protected def getPartitions: Array[Partition] = {
    redisSlots.zipWithIndex.map {
      case (slot, idx) =>
        new KVStorePartition(idx, slot.ip, slot.port)
    }

  }

  override def compute(split: Partition, context: TaskContext): Iterator[(DNASeq, Int)] = {
    val partition: KVStorePartition = split.asInstanceOf[KVStorePartition]
    val ips = JavaUtils.getAllIPs.map(_.getHostAddress)
    if (!ips.contains(partition.ip)) {
      logger.info(s"partation ip ${partition.ip} is not the node ip ${ips.mkString(",")}")
    }
    val kvstore = KVStoreManagerSingleton.instance(redisSlots).getKVStoreClient(partition.slot)
    val resp = kvstore.get_kmer_counts(useBloomFilter = useBloomFilter, minimumCount = 1)
    kvstore.close()
    resp.toIterator
  }
}