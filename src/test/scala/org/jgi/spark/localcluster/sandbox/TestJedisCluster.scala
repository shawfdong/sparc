package org.jgi.spark.localcluster.sandbox

import java.util

import org.jgi.spark.localcluster.RedisClusterUnitSuite
import org.junit.Test
import org.scalatest.Matchers
import redis.clients.jedis.{HostAndPort, JedisCluster}
/**
  * Created by Lizhen Shi on 5/21/17.
  */
class TestJedisCluster extends RedisClusterUnitSuite with Matchers  {



  @Test def testCalculateConnectionPerSlot(): Unit = {
    val jedisClusterNode = new util.HashSet[HostAndPort]();
    jedisClusterNode.add(new HostAndPort("127.0.0.1", 7379))
    val jc = new JedisCluster(jedisClusterNode)
    jc.set("foo", "bar")
    jc.set("test", "test")
    node3.get("foo") shouldEqual "bar"
    node2.get("test") shouldEqual "test"

  }
}
