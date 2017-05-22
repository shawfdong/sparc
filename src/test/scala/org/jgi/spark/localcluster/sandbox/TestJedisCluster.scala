package org.jgi.spark.localcluster.sandbox

import java.util

import org.jgi.spark.localcluster.{RedisClusterUnitSuite, RedisUtil}
import org.junit.Test
import org.scalatest.Matchers
import redis.clients.jedis.{HostAndPort, JedisCluster}
/**
  * Created by Lizhen Shi on 5/21/17.
  */
class TestJedisCluster extends RedisClusterUnitSuite with Matchers  {



  @Test def testCalculateConnectionPerSlot(): Unit = {
    println("start test on testCalculateConnectionPerSlot")
    cluster.set("foo", "bar")
    cluster.set("test", "test")
    cluster.get("foo") shouldEqual "bar"
    cluster.get("test") shouldEqual "test"

    RedisUtil.get_cluster_nodes(cluster).foreach(println)


  }
}
