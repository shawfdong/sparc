package org.jgi.spark.localcluster

import java.io.{File, FilenameFilter}
import java.util
import java.util.{List, UUID}

import com.holdenkarau.spark.testing.{SharedJavaSparkContext, SharedSparkContext}
import org.apache.spark.SparkConf
import org.junit.BeforeClass
import org.scalatest.junit.JUnitSuite
import redis.clients.jedis.{HostAndPort, JedisCluster}
import org.junit.After
import org.junit.AfterClass
import org.scalatest.FunSpec
import redis.embedded.{Redis, RedisServer}
import redis.embedded.cluster.{RedisCluster, RedisClusterModifier}

import collection.JavaConversions._
import scala.language.implicitConversions


/**
  * Created by Lizhen Shi on 5/21/17.
  */
abstract class RedisClusterUnitSuite extends JUnitSuite  {

  def sc = RedisClusterUnitSuite.auxClass.sc

  def jedisMgr = RedisClusterUnitSuite.jedisMgr

  @After
  def tearDown(): Unit = {

    jedisMgr.flushAll()
    if (false) {
      jedisMgr.jedis_pool_ins_map.values
       .  foreach {
          pool  =>
          val jedis = pool.getResource
          if (!jedis.info.contains("role:slave"))
            jedis.flushAll()
          jedis.close()
      }

    }
  }
}


object RedisClusterUnitSuite extends JUnitSuite {
  val ports = Array(42000, 42001, 42002).map(i => i: java.lang.Integer)

  class AuxClass extends  FunSpec with    SharedSparkContext {

    override def conf: SparkConf ={
      super.conf.set("spark.ui.enabled", "true")
    }
  }

  var jedisMgr: JedisManager = _

  val tmp_dir = "/tmp"

  var redis_home: String = null

  val auxClass=new AuxClass

  var servers:Array[RedisServer] = null
  @BeforeClass
  def beforeClass(): Unit = {
    redis_home = System.getenv("REDIS_HOME")

    if (redis_home == null) throw new RuntimeException("REDIS_HOME is not set")
    System.setProperty("REDIS_HOME", redis_home)


    import redis.embedded.RedisExecProvider
    import redis.embedded.util.OS
    val provider = RedisExecProvider.build().`override`(OS.UNIX, redis_home + "/src/redis-server")


    servers = ports.map{
      port=>
      new RedisServer.Builder()
        .redisExecProvider(provider)
        .port(port )
        .build()
    }

    servers.foreach(_.start())

    jedisMgr = new JedisManager(ports.map(x => ("127.0.0.1", x.toInt)).toSet)
    auxClass.beforeAll()

  }


  @AfterClass def cleanUp(): Unit = {
    if (servers !=null) servers.foreach(_.stop())
    if (redis_home != null) RedisClusterModifier.delete_files(redis_home + "/src/", ".*4200.*")
    auxClass.afterAll()
  }


}
