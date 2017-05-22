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

  val hosts_map: Predef.Map[String, Array[Int]] = hostsAndPorts.groupBy(x => x._1).map(x => (x._1, x._2.map(_._2).toArray))

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
    val ip = JavaUtils.getMatchedIP(hosts_map.keys.toSeq.asJava)
    val ports = hosts_map.get(ip)
    ports match {
      case Some(x) =>
        val random_index = rand.nextInt(x.length)
        val port = x(random_index)
        getJedis(ip, port)
      case None =>
        throw new Exception(s"cannot find port for $ip")
    }

  }

  val getJedisCluster: JedisCluster = {
    val ip = JavaUtils.getMatchedIP(hosts_map.keys.toSeq.asJava)
    val ports = if (ip == null || !hosts_map.contains(ip))
      hosts_map.flatMap {
        x =>
          x._2.map((x._1, _))
      }.toArray
    else
      hosts_map.get(ip) match {
        case Some(x) => x.map((ip, _))
        case None => throw new Exception("never be here")
      }

    val hostsAndPorts = new util.HashSet[HostAndPort]
    scala.util.Random.shuffle(ports.toSeq).map(x => new HostAndPort(x._1, x._2)).toSet.foreach {
      x: HostAndPort => hostsAndPorts.add(x)
    }
    new JedisCluster(hostsAndPorts)

  }


  def flushAll(): Unit = {
    hosts_map.foreach {
      case (ip, ports: Array[Int]) =>
        ports.foreach {
          i =>
            val jedis = getJedis(ip, i)
            if (!jedis.info.contains("role:slave")) {
              jedis.flushAll()
              logger.debug(s"flush node $ip, $i")
            }
            jedis.close()
        }
    }
  }
}
