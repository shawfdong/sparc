package org.jgi.spark.localcluster.rdd

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.jgi.spark.localcluster.myredis.{JedisManagerSingleton, RedisSlot}
import org.jgi.spark.localcluster.{Constant, SingleEdge}

/**
  * Created by Lizhen Shi on 5/23/17.
  */
class RedisEdgeBloomFilterCountRDD
(sc: SparkContext, redisSlots: Array[RedisSlot])
  extends RedisEdgeCountRDD(sc, redisSlots) with LazyLogging {

  override def count(): Long = {
    val counts = this.partitions.map {
      p =>
        val partition = p.asInstanceOf[RedisPartition]
        val jedis = JedisManagerSingleton.instance(redisSlots).getJedis(partition.slot)
        val n = jedis.get(partition.slot.key(Constant.EDGE_COUNTING_REDIS_BLOOMFILTER_COUNT_HASH_KEY))
        jedis.close()
        n.toInt //currently max key number is 2^32
    }
    counts.sum
  }

  override def compute(split: Partition, context: TaskContext): Iterator[(SingleEdge, Int)] = {
    super.compute(split, context).map(x => (x._1, x._2 + 1))
  }
}