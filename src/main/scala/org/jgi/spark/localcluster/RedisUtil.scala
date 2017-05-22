package org.jgi.spark.localcluster

import redis.clients.jedis.{JedisCluster}
import collection.JavaConversions._

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
  def get_cluster_nodes(cluster: JedisCluster) = {
    cluster.getClusterNodes.map {
      case (name, pool) =>
        val jedis = pool.getResource
        val info=jedis.clusterNodes()
        jedis.close()
        (name,info)
    }
  }

}
