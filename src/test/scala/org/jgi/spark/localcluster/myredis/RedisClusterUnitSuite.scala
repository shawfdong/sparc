package org.jgi.spark.localcluster.myredis

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.SparkConf
import org.junit.{After, AfterClass, BeforeClass}
import org.scalatest.FunSpec
import org.scalatest.junit.JUnitSuite
import redis.embedded.cluster.RedisClusterModifier
import redis.embedded.util.OS
import redis.embedded.{RedisExecProvider, RedisServer}

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
