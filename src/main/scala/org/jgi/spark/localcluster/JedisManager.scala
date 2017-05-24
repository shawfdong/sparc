package org.jgi.spark.localcluster

import java.util

import com.typesafe.scalalogging.LazyLogging
import redis.clients.jedis._
import redis.clients.util.SafeEncoder

import collection.JavaConverters._
import collection.mutable._
import scala.collection.mutable
import scala.util.Random

/**
  * Created by Lizhen Shi on 5/21/17.
  */
class JedisManager(hostsAndPortsSet: collection.immutable.Set[(String, Int)], n_slot_per_ins: Int = 2) extends LazyLogging {

  val _hostsAndPorts = hostsAndPortsSet.toArray
  val redisSlots = _hostsAndPorts.sorted.map(x => 0.until(n_slot_per_ins).map(_ => x))
    .flatten.zipWithIndex.map { case ((ip, port), idx) => RedisSlot(ip, port, idx) }

  def this(ip: String, port: Int) = {

    this {
      val jedis = new Jedis(ip, port)
      val info = jedis.clusterNodes()
      println(info)
      val hostsAndIps = info.split("\n|\r").map(_.split(" ").map(_.trim()).filter(_.nonEmpty))
        .map(_.take(2)(1)).map(_.split(":")).map(x => (x(0), x(1).toInt))
      hostsAndIps.foreach(println)
      jedis.close()
      hostsAndIps.toSet
    }
  }

  val rand = new Random(System.currentTimeMillis())

  private def get_pool(slot: RedisSlot): Option[JedisPool] = {
    get_pool(slot.ip, slot.port)
  }

  private def get_pool(ip: String, port: Int): Option[JedisPool] = {
    val ipAndPort = (ip, port)
    if (!jedis_pool_ins_map.contains(ipAndPort)) {

      val poolConfig = new JedisPoolConfig()
      poolConfig.setMaxTotal(256); // maximum active connections
      jedis_pool_ins_map.put(ipAndPort, new JedisPool(poolConfig, ip, port))
    }
    jedis_pool_ins_map.get(ipAndPort)
  }

  def getJedis(ip: String, port: Int): Jedis = {
    get_pool(ip, port) match {
      case Some(pool) => pool.getResource
      case None => throw new Exception("never be here")
    }
  }

  def getSlot(keyHash: Int): RedisSlot = {
    redisSlots(keyHash % redisSlots.length)
  }


  def getJedis(slot: RedisSlot): Jedis = {
    get_pool(slot) match {
      case Some(pool) => pool.getResource
      case None => throw new Exception("never be here")
    }
  }

  def flushAll(): Unit = {
    _hostsAndPorts.indices.foreach {
      i =>
        val slot=getSlot(i)
        val jedis = getJedis(slot)
        if (!jedis.info.contains("role:slave")) {
          val ip = _hostsAndPorts(i)._1
          val port = _hostsAndPorts(i)._2
          var code = jedis.flushAll()
          logger.debug(s"flush node $ip, $port, response $code")

          if (!"OK".equals(code)) {
            code = jedis.flushAll()
            logger.debug(s"flush node $ip, $port, response $code")
          }
        }
        jedis.close()
    }
  }


  def set(k: String, v: String, hash_fun: String => Int = null): Unit = {
    val hashVal = if (hash_fun == null) k.hashCode() else hash_fun(k)
    val slot=getSlot(hashVal)
    val jedis = getJedis(slot)

    try {
      jedis.set(k, v)
    } finally {
      jedis.close()
    }
  }

  def set_batch(keys: collection.immutable.Iterable[String], values: collection.immutable.Iterable[String],
                provided_hash_fun: String => Int = null): Unit = {
    val hash_fun = if (provided_hash_fun == null) (x: String) => x.hashCode() else provided_hash_fun
    if (keys.size != values.size) throw new Exception("key value must be same size")
    keys.zip(values).map {
      case (a, b) =>
        (hash_fun(a), a, b)
    }.groupBy(_._1).foreach {
      case (hashVal, grouped) =>
        val slot=getSlot(hashVal)
        val jedis = getJedis(slot)

        val p = jedis.pipelined()
        try {
          grouped.foreach {
            case (_, k, v) =>
              p.set(k, v)
          }
        } finally {
          jedis.close()
        }
    }
  }

  def incr(k: String, hash_fun: String => Int = null): Unit = {
    val hashVal = if (hash_fun == null) k.hashCode() else hash_fun(k)
    val slot=getSlot(hashVal)
    val jedis = getJedis(slot)

    try {
      jedis.incr(k)
    } finally {
      jedis.close()
    }
  }

  def incr(seq: DNASeq): Unit = {
    val hashVal = seq.hashCode()
    val slot=getSlot(hashVal)
    val jedis = getJedis(slot)

    try
      jedis.hincrBy(SafeEncoder.encode(slot.key("hkc")), seq.bytes, 1)
    finally {
      jedis.close()
    }
  }



  def incr_batch(keys: collection.Iterable[DNASeq]): Unit = {
    keys.map { x => (x .hashCode % redisSlots.length, x) }
      .groupBy(_._1).foreach {
      case (hashVal, grouped) =>
        val slot=getSlot(hashVal)
        val jedis = getJedis(slot)
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
    val slot=getSlot(hv)
    val jedis = getJedis(slot)
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

  def get(k: String, hash_fun: String => Int = null): String = {
    val hashVal = if (hash_fun == null) k.hashCode() else hash_fun(k)
    val slot=getSlot(hashVal)
    val jedis = getJedis(slot)
    try {
      jedis.get(k)
    } finally {
      jedis.close()
    }
  }


  @transient val jedis_pool_ins_map: HashMap[(String, Int), JedisPool] = new HashMap[(String, Int), JedisPool]()

  def close(): Unit = {
    if (jedis_pool_ins_map.size > 0) {
      logger.info("JedisManager: jvm exiting, destroying jedis pool")
      jedis_pool_ins_map.values.foreach(_.destroy())
    }
  }

  Runtime.getRuntime.addShutdownHook(new Thread(new Runnable() {
    def run(): Unit = {
      close()
    }
  }))

  logger.info("create Jedis manager with slots:\n" + redisSlots.map(_.toString).mkString("\n")
  )

}

object JedisManagerSingleton extends LazyLogging {
  @transient private var _instance: JedisManager = null
  private var _hostsAndPorts: Array[(String, Int)] = null

  def instance(hostsAndPorts: Array[(String, Int)]): JedisManager = {
    if (_instance == null) {
      logger.info("create singlton instance for the first time.")
      logger.info(hostsAndPorts.map(x => x._1 + ":" + x._2.toString).mkString(" "))

      _instance = new JedisManager(hostsAndPorts.toSet)
    } else if (!hostsAndPorts.sameElements(_hostsAndPorts)) {

      logger.info("re-create singlton instance for a different set of hosts and ports")
      logger.info(hostsAndPorts.map(x => x._1 + ":" + x._2.toString).mkString(" "))

      _instance.close()
      _instance = new JedisManager(hostsAndPorts.toSet)
    }
    _hostsAndPorts = hostsAndPorts
    _instance
  }
}