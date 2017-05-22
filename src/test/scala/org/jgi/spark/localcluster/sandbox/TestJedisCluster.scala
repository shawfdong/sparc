package org.jgi.spark.localcluster.sandbox

import java.util

import org.jgi.spark.localcluster.{JedisManager, RedisClusterUnitSuite, RedisUtil}
import org.junit.Test
import org.scalatest.Matchers
import redis.clients.jedis.{HostAndPort, JedisCluster}

/**
  * Created by Lizhen Shi on 5/21/17.
  */
class TestJedisCluster extends RedisClusterUnitSuite with Matchers {


  @Test def testCluster(): Unit = {
    println("start test on testCluster")
    cluster.set("foo", "bar")
    cluster.set("test", "test")
    cluster.get("foo") shouldEqual "bar"
    cluster.get("test") shouldEqual "test"

    RedisUtil.get_cluster_nodes(cluster).foreach(println)


  }

  @Test def testInitialOneNode(): Unit = {
    val mgr = new JedisManager("127.0.0.1", 42005)
    val cluster = mgr.getJedisCluster
    cluster.set("foo", "bar")
    cluster.set("test", "test")
    cluster.get("foo") shouldEqual "bar"
    cluster.get("test") shouldEqual "test"
    mgr.close()
  }

}
