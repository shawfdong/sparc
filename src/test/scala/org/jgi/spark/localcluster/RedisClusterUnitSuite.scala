package org.jgi.spark.localcluster

import org.junit.BeforeClass
import org.scalatest.junit.JUnitSuite
import redis.clients.jedis.{Jedis, JedisPoolConfig}
import org.junit.After
import org.junit.AfterClass
import redis.clients.jedis.JedisCluster.Reset
import redis.clients.jedis.JedisCluster

/**
  * Created by Lizhen Shi on 5/21/17.
  */
class RedisClusterUnitSuite extends JUnitSuite {
  def node1 = RedisClusterUnitSuite.node1

  def node2 = RedisClusterUnitSuite.node2

  def node3 = RedisClusterUnitSuite.node3

  @After
  def tearDown(): Unit = {
    RedisClusterUnitSuite.cleanUp()
  }
}


object RedisClusterUnitSuite extends JUnitSuite {
  private val nodeInfo1 = HostAndPortUtil.getClusterServers.get(0)
  private val nodeInfo2 = HostAndPortUtil.getClusterServers.get(1)
  private val nodeInfo3 = HostAndPortUtil.getClusterServers.get(2)
  private val nodeInfo4 = HostAndPortUtil.getClusterServers.get(3)
  private val nodeInfoSlave2 = HostAndPortUtil.getClusterServers.get(4)

  private var node1 = new Jedis(nodeInfo1.getHost, nodeInfo1.getPort)
  private val node2 = new Jedis(nodeInfo2.getHost, nodeInfo2.getPort)
  private val node3 = new Jedis(nodeInfo3.getHost, nodeInfo3.getPort)
  private val node4 = new Jedis(nodeInfo4.getHost, nodeInfo4.getPort)
  private val nodeSlave2 = new Jedis(nodeInfoSlave2.getHost, nodeInfoSlave2.getPort)
  private val localHost = "127.0.0.1"

  private val DEFAULT_TIMEOUT = 2000
  private val DEFAULT_REDIRECTIONS = 5
  private val DEFAULT_CONFIG = new JedisPoolConfig


  @BeforeClass
  def beforeClass(): Unit = {
    node1.auth("cluster")
    node1.flushAll
    node2.auth("cluster")
    node2.flushAll
    node3.auth("cluster")
    node3.flushAll
    node4.auth("cluster")
    node4.flushAll
    nodeSlave2.auth("cluster")
    nodeSlave2.flushAll
    // ---- configure cluster
    // add nodes to cluster
    node1.clusterMeet(RedisClusterUnitSuite.localHost, RedisClusterUnitSuite.nodeInfo2.getPort)
    node1.clusterMeet(RedisClusterUnitSuite.localHost, RedisClusterUnitSuite.nodeInfo3.getPort)
    // split available slots across the three nodes

    val slotsPerNode = JedisCluster.HASHSLOTS / 3
    val node1Slots = new Array[Int](slotsPerNode)
    val node2Slots = new Array[Int](slotsPerNode + 1)
    val node3Slots = new Array[Int](slotsPerNode)
    var i = 0
    var slot1 = 0
    var slot2 = 0
    var slot3 = 0
    while ( {
      i < JedisCluster.HASHSLOTS
    }) {
      if (i < slotsPerNode) node1Slots({
        slot1 += 1;
        slot1 - 1
      }) = i
      else if (i > slotsPerNode * 2) node3Slots({
        slot3 += 1;
        slot3 - 1
      }) = i
      else node2Slots({
        slot2 += 1;
        slot2 - 1
      }) = i

      {
        i += 1;
        i - 1
      }
    }
    node1.clusterAddSlots(node1Slots: _*)
    node2.clusterAddSlots(node2Slots: _*)
    node3.clusterAddSlots(node3Slots: _*)
    JedisClusterTestUtil.waitForClusterReady(node1, node2, node3)
  }


  @AfterClass def cleanUp(): Unit = {
    node1.flushDB
    node2.flushDB
    node3.flushDB
    node4.flushDB
    node1.clusterReset(Reset.SOFT)
    node2.clusterReset(Reset.SOFT)
    node3.clusterReset(Reset.SOFT)
    node4.clusterReset(Reset.SOFT)
  }


}
