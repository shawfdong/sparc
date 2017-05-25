package org.jgi.spark.localcluster.myredis

import org.jgi.spark.localcluster.{Constant, DNASeq, JavaUtils}
import redis.clients.jedis.Protocol.toByteArray
import redis.clients.jedis.{Client, Pipeline, Transaction}
import redis.clients.util.SafeEncoder

import scala.collection.JavaConversions._

/**
  * Created by Lizhen Shi on 5/24/17.
  */
class RedisFunction(@transient val mgr: JedisManager) {

  def incr(seq: DNASeq): Unit = {
    val hashVal = seq.hashCode()
    val slot = mgr.getSlot(hashVal)
    val jedis = mgr.getJedis(slot)

    try
      jedis.hincrBy(SafeEncoder.encode(slot.key(Constant.KMER_COUNTING_REDIS_HASH_KEY)), seq.bytes, 1)
    finally {
      jedis.close()
    }
  }


  def incr_batch(keys: collection.Iterable[DNASeq]): Unit = {
    keys.map { x => (x.hashCode % mgr.redisSlots.length, x) }
      .groupBy(_._1).foreach {
      case (hashVal, grouped) =>
        val slot = mgr.getSlot(hashVal)
        val jedis = mgr.getJedis(slot)
        val p = jedis.pipelined()
        val slotkey = SafeEncoder.encode(slot.key(Constant.KMER_COUNTING_REDIS_HASH_KEY))
        try {
          grouped.foreach {
            case (_, k) =>
              p.hincrBy(slotkey, k.bytes, 1)
          }
          p.sync()
        } finally {
          jedis.close()
        }
    }
  }


  def get_client(p: Pipeline, key: Array[Byte]): Client = {
    val ret = JavaUtils.genericInvokMethod(p, "getClient", 1, key)
    ret.asInstanceOf[Client]
  }

  def get_client(p: Transaction, key: Array[Byte]): Client = {
    val ret = JavaUtils.genericInvokMethod(p, "getClient", 1, key)
    ret.asInstanceOf[Client]
  }

  //bloom filter
  def bf_incr_batch(keys: collection.Iterable[DNASeq]): Unit = {
    val bf_size = SafeEncoder.encode(1000000.toString)
    val fp_rate = SafeEncoder.encode(0.01.toString)
    keys.map { x => (x.hashCode % mgr.redisSlots.length, x) }
      .groupBy(_._1).foreach {
      case (hashVal, grouped) =>
        val slot = mgr.getSlot(hashVal)
        val jedis = mgr.getJedis(slot)
        val bfkey = SafeEncoder.encode(slot.key(Constant.KMER_COUNTING_REDIS_BLOOMFILTER_HASH_KEY))
        val slotkey = SafeEncoder.encode(slot.key(Constant.KMER_COUNTING_REDIS_HASH_KEY))
        val sha = SafeEncoder.encode(LuaScript.get_sha(LuaScript.CAS_HINCR, jedis, slot.instance_id))
        val p = jedis.pipelined()
        val client = get_client(p, sha)
        //        val p = jedis.multi()
        //        val client = get_client(p, sha)
        try {
          grouped.foreach {
            case (group_id, k) =>
              client.evalsha(sha, toByteArray(0), bfkey, bf_size, fp_rate, slotkey, k.bytes) //ensure 7 parameters
          }
          val returns = client.getAll()
          if (false) //debug
          {
            val ins = grouped.toArray
            if (returns.length != ins.length) throw new Exception("#requests <> #responses")
            ins.indices.map {
              i =>
                val a = ins(i)._2.to_base64
                val x = "iIiIiIiIiIg="
                //val x = "ERERERERERA="
                if (a.equals(x)) {
                  val hcount = {
                    val aa = jedis.hget(slotkey, ins(i)._2.bytes)
                    if (aa == null) aa else SafeEncoder.encode(aa)
                  }
                  println(s"AAAAAAAAA, send $x to $hashVal, ${slot.toString}, return  ${returns(i)} ${returns(i).getClass} , count $hcount ")
                }
            }
          }
          p.sync()
        } finally {
          jedis.close()
        }
    }
  }

  def get(s: DNASeq): String = {
    val hv = s.hashCode
    val slot = mgr.getSlot(hv)
    val jedis = mgr.getJedis(slot)
    try {
      val bytes = jedis.hget(SafeEncoder.encode(slot.key(Constant.KMER_COUNTING_REDIS_HASH_KEY)), s.bytes)
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
  implicit def toRedisFunction(mgr: JedisManager): RedisFunction = new RedisFunction(mgr)
}
