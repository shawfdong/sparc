package org.jgi.spark.localcluster.myredis

import redis.clients.jedis.JedisCluster

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Created by Lizhen Shi on 5/21/17.
  */
object RedisUtil {
  def get_connection(cluster:JedisCluster): Unit ={
    cluster.getClusterNodes.foreach{
      case(name,pool)=>
        println(name)
    }
  }
  def get_cluster_nodes(cluster: JedisCluster): mutable.Map[String, String] = {
    cluster.getClusterNodes.map {
      case (name, pool) =>
        val jedis = pool.getResource
        val info=jedis.clusterNodes()
        jedis.close()
        (name,info)
    }
  }

}
