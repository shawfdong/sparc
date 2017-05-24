package org.jgi.spark.localcluster.myredis

import org.jgi.spark.localcluster.DNASeq
import redis.clients.util.SafeEncoder

/**
  * Created by Lizhen Shi on 5/24/17.
  */
class RedisFunction(@transient val mgr:JedisManager ) {

  def incr(seq: DNASeq): Unit = {
    val hashVal = seq.hashCode()
    val slot=mgr.getSlot(hashVal)
    val jedis = mgr.getJedis(slot)

    try
      jedis.hincrBy(SafeEncoder.encode(slot.key("hkc")), seq.bytes, 1)
    finally {
      jedis.close()
    }
  }



  def incr_batch(keys: collection.Iterable[DNASeq]): Unit = {
    keys.map { x => (x .hashCode % mgr.redisSlots.length, x) }
      .groupBy(_._1).foreach {
      case (hashVal, grouped) =>
        val slot=mgr.getSlot(hashVal)
        val jedis = mgr.getJedis(slot)
        val p = jedis.pipelined()
        try {
          grouped.foreach {
            case (_, k) =>
              p.hincrBy(SafeEncoder.encode(slot.key("hkc")), k.bytes, 1)
          }
          p.sync()
        } finally {
          jedis.close()
        }
    }
  }

  def get(s: DNASeq): String = {
    val hv = s.hashCode
    val slot=mgr.getSlot(hv)
    val jedis = mgr.getJedis(slot)
    try {
      val bytes = jedis.hget(SafeEncoder.encode(slot.key("hkc")), s.bytes)
      if (bytes == null)
        null
      else
        SafeEncoder.encode(bytes)
    } finally {
      jedis.close()
    }
  }
}

trait RedisFunTrait {
  implicit def toRedisFunction(mgr:JedisManager): RedisFunction = new RedisFunction(mgr)
}
