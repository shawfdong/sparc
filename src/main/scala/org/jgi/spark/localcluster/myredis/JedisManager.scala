package org.jgi.spark.localcluster.myredis

import com.typesafe.scalalogging.LazyLogging
import redis.clients.jedis._

import scala.collection.mutable._
import scala.util.Random

/**
  * Created by Lizhen Shi on 5/21/17.
  */
//class JedisManager(hostsAndPortsSet: collection.immutable.Set[(String, Int)], n_slot_per_ins: Int = 2) extends LazyLogging {
class JedisManager(val redisSlots: Array[RedisSlot]) extends LazyLogging {

  val _hostsAndPorts: Array[(String, Int)] = redisSlots.map(x => (x.ip, x.port)).distinct

  def this(hostsAndPortsSet: collection.immutable.Set[(String, Int)], n_slot_per_ins: Int = 2) = {
    this {
      hostsAndPortsSet.toArray.sorted.map(x => 0.until(n_slot_per_ins).map(_ => x))
        .flatten.zipWithIndex.map { case ((ip, port), idx) => RedisSlot(ip, port, idx) }
    }
  }


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
      poolConfig.setMaxTotal(256) // maximum active connections
      poolConfig.setMaxWaitMillis(30 * 1000)
      jedis_pool_ins_map.put(ipAndPort, new JedisPool(poolConfig, ip, port, 30 * 1000))
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
        val ip = _hostsAndPorts(i)._1
        val port = _hostsAndPorts(i)._2
        val jedis = getJedis(ip, port)
        if (!jedis.info.contains("role:slave")) {
          var code = jedis.bf_flush_all()
          logger.debug(s"flush node $ip, $port, response $code")

          if (!"OK".equals(code)) {
            code = jedis.bf_flush_all()
            logger.debug(s"flush node $ip, $port, response $code")
          }
        }
        jedis.close()
    }
  }

  def loadModule(soPath:String):Unit ={
    _hostsAndPorts.indices.foreach {
      i =>
        val ip = _hostsAndPorts(i)._1
        val port = _hostsAndPorts(i)._2
        val jedis = getJedis(ip, port)
        if (!jedis.info.contains("role:slave")) {
          var code = jedis.load_module(soPath);
          logger.info(s"Redis load module $soPath, response $code")
        }
        jedis.close()
    }
  }


  def set(k: String, v: String, hash_fun: String => Int = null): Unit = {
    val hashVal = if (hash_fun == null) k.hashCode() else hash_fun(k)
    val slot = getSlot(hashVal)
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
        val slot = getSlot(hashVal)
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
    val slot = getSlot(hashVal)
    val jedis = getJedis(slot)

    try {
      jedis.incr(k)
    } finally {
      jedis.close()
    }
  }


  def get(k: String, hash_fun: String => Int = null): String = {
    val hashVal = if (hash_fun == null) k.hashCode() else hash_fun(k)
    val slot = getSlot(hashVal)
    val jedis = getJedis(slot)
    try {
      jedis.get(k)
    } finally {
      jedis.close()
    }
  }


  @transient val jedis_pool_ins_map: HashMap[(String, Int), JedisPool] = new HashMap[(String, Int), JedisPool]()

  def close(): Unit = {
    if (jedis_pool_ins_map.nonEmpty) {
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

  def instance(redis_ip_ports: Array[(String, Int)], n_redis_slot: Int): JedisManager = {
    if (_instance == null) {

      _instance = new JedisManager(redis_ip_ports.toSet, n_redis_slot)
      logger.info("create singlton instance for the first time.")
      logger.info(_instance.redisSlots.map(x => x.toString).mkString("\n"))

    }
    _instance
  }

  def instance: JedisManager = _instance

  def instance(redisSlots: Array[RedisSlot]): JedisManager = {
    if (_instance == null) {
      logger.info("create singlton instance for the first time.")
      logger.info(redisSlots.map(x => x.toString).mkString("\n"))

      _instance = new JedisManager(redisSlots)
    }
    _instance
  }
}
