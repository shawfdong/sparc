package org.jgi.spark.localcluster.kvstore

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, AfterClass, BeforeClass}
import org.scalatest.FunSpec
import org.scalatest.junit.JUnitSuite
import redis.embedded.RedisServer

import scala.language.implicitConversions


/**
  * Created by Lizhen Shi on 6/2/17.
  */
abstract class KVStoreClusterUnitSuite extends JUnitSuite {

  def sc: SparkContext = KVStoreClusterUnitSuite.auxClass.sc

  def kvstoreMgr: KVStoreManager = KVStoreClusterUnitSuite.kvstoreMgr

  @After
  def tearDown(): Unit = {

    kvstoreMgr.flushAll()
    if (false) {
      kvstoreMgr.kvstore_pool_ins_map.values
        .foreach {
          pool =>
            val kvstore = pool.getResource
            kvstore.flushAll()
            kvstore.close()
        }
    }
  }
}


object KVStoreClusterUnitSuite extends JUnitSuite {
  val ports: Array[Integer] = Array(42000, 42001, 42002).map(i => i: java.lang.Integer)

  class AuxClass extends FunSpec with SharedSparkContext {

    override def conf: SparkConf = {
      super.conf.set("spark.ui.enabled", "true")
    }
  }

  var kvstoreMgr: KVStoreManager = _

  val auxClass = new AuxClass

  var servers: Array[KVStoreLocalhostServer] = null

  @BeforeClass
  def beforeClass(): Unit = {
    servers = ports.map {
      port =>
        new KVStoreLocalhostServer(port)
    }

    servers.foreach(x=> new Thread(x).start())

    kvstoreMgr = new KVStoreManager(ports.map(x => ("127.0.0.1", x.toInt)).toSet)
    auxClass.beforeAll()

  }


  @AfterClass def cleanUp(): Unit = {
    if (servers != null) servers.foreach(_.shutdown())
    auxClass.afterAll()
  }


}
