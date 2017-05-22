package org.jgi.spark.localcluster

import java.util

import com.typesafe.scalalogging.LazyLogging
import redis.clients.jedis
import redis.clients.jedis._

import collection.JavaConverters._
import collection.mutable._
import scala.util.Random

/**
  * Created by Lizhen Shi on 5/21/17.
  */
class JedisManager(val hostsAndPorts: collection.immutable.Set[(String, Int)]) extends LazyLogging {

  val rand = new Random(System.currentTimeMillis())

  val jedis_pool_map: HashMap[String, JedisPool] = new HashMap[String, JedisPool]()

  def get_pool(ip: String, port: Int): Option[JedisPool] = {
    val k: String = s"$ip $port"
    if (!jedis_pool_map.contains(k))
      jedis_pool_map.put(k, new JedisPool(ip, port))

    jedis_pool_map.get(k)
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


  def close(): Unit = {
    jedis_pool_map.values.foreach(_.destroy())
  }

  def getJedis(ip: String, port: Int): Jedis = {
    get_pool(ip, port) match {
      case Some(pool) => pool.getResource
      case None => throw new Exception("never be here")
    }
  }

  def getJedis: Jedis = {
    val ip = JavaUtils.getMatchedIP(hostsAndPorts.map(_._1).toSeq.asJava)
    val local_ports = hostsAndPorts.filter(_._1 == ip)
    val ports =
      if (local_ports.nonEmpty)
        local_ports.toList
      else
        hostsAndPorts.toList

    val random_index = rand.nextInt(ports.length)
    val port = ports(random_index)
    getJedis(port._1, port._2)

  }

  def getJedisCluster: JedisCluster = {
    val ip = JavaUtils.getMatchedIP(hostsAndPorts.map(_._1).toSeq.asJava)
    val local_ports = hostsAndPorts.filter(_._1 == ip)
    val ports =
      if (local_ports.nonEmpty)
        local_ports.toList
      else
        hostsAndPorts.toList

    val hostsAndPorts2 = new util.HashSet[HostAndPort]
    scala.util.Random.shuffle(ports.toSeq).map(x => new HostAndPort(x._1, x._2)).toSet.foreach {
      x: HostAndPort => hostsAndPorts2.add(x)
    }
    new JedisCluster(hostsAndPorts2)

  }

  def getSeedNode(): (String, Int) = {
    Random.shuffle(this.hostsAndPorts).head
  }

  def flushAll(): Unit = {
    hostsAndPorts.foreach {
      case (ip, port) =>
        val jedis = getJedis(ip, port)
        if (!jedis.info.contains("role:slave")) {
          jedis.flushAll()
          logger.debug(s"flush node $ip, $port")
        }
        jedis.close()
    }
  }
}
