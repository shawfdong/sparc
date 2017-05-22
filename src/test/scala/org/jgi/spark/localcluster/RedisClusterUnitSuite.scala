package org.jgi.spark.localcluster

import java.io.{File, FilenameFilter}
import java.util
import java.util.{List, UUID}

import org.junit.BeforeClass
import org.scalatest.junit.JUnitSuite
import redis.clients.jedis.{HostAndPort, JedisCluster}
import org.junit.After
import org.junit.AfterClass
import redis.embedded.{Redis, RedisServer}
import redis.embedded.cluster.{RedisCluster, RedisClusterModifier}

import collection.JavaConversions._
import scala.language.implicitConversions


/**
  * Created by Lizhen Shi on 5/21/17.
  */
class RedisClusterUnitSuite extends JUnitSuite {
  def cluster = RedisClusterUnitSuite.jedisMgr.jedisCluster

  @After
  def tearDown(): Unit = {
    cluster.getClusterNodes.foreach {
      case (name, pool) =>
        println(s"flush node $name")
        val jedis = pool.getResource
        if (!jedis.info.contains("role:slave"))
          jedis.flushAll()
        jedis.close()
    }

  }
}


object RedisClusterUnitSuite extends JUnitSuite {
  private var cluster: RedisCluster = _
  var jedisMgr: JedisManager = _

  val tmp_dir = "/tmp"

  var redis_home: String = _

  ///redis_cluster_test_"+UUID.randomUUID()
  @BeforeClass
  def beforeClass(): Unit = {

    redis_home = System.getenv("REDIS_HOME")
    if (redis_home == null) throw new RuntimeException("REDIS_HOME is not set")
    System.setProperty("REDIS_HOME", redis_home)


    import redis.embedded.RedisExecProvider
    import redis.embedded.util.OS
    val provider = RedisExecProvider.build().`override`(OS.UNIX, redis_home + "/src/redis-server")

    val ports = Array(42000, 42001, 42002, 42003, 42004, 42005).map(i => i: java.lang.Integer)


    cluster = new RedisCluster.Builder()
      .withServerBuilder(new RedisServer.Builder().redisExecProvider(provider).setting(s"dir $tmp_dir"))
      .serverPorts(java.util.Arrays.asList(ports: _*))
      .numOfReplicates(1)
      .numOfMasters(ports.length)
      .numOfRetries(5)
      .build()
    try {
      val modifier = new RedisClusterModifier(cluster)
      modifier.start()
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    jedisMgr=new JedisManager(ports.map(x=>("127.0.0.1",x.toInt)).toSet)

  }


  @AfterClass def cleanUp(): Unit = {
    jedisMgr.close()
    cluster.stop()
    if (redis_home !=null) RedisClusterModifier.delete_files(redis_home + "/src/", ".*4200.*")

  }


}
