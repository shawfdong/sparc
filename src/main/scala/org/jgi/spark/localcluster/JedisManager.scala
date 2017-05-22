package org.jgi.spark.localcluster

import java.util

import redis.clients.jedis
import redis.clients.jedis.{HostAndPort, JedisCluster}

/**
  * Created by Lizhen Shi on 5/21/17.
  */
class JedisManager(val hostsAndPorts: Set[(String, Int)]) {
  val jedisCluster: JedisCluster = {
    val jedisClusterNodes = new util.HashSet[HostAndPort]()
    hostsAndPorts.map(x => new jedis.HostAndPort(x._1, x._2)).foreach {
      jedisClusterNodes.add(_)
    }
    new JedisCluster(jedisClusterNodes)
  }

  val hosts_map= hostsAndPorts.groupBy(x=>x._1).map(x=> (x._1,x._2.map(_._2).toArray))

  def close(): Unit ={
    jedisCluster.close()
  }

}
