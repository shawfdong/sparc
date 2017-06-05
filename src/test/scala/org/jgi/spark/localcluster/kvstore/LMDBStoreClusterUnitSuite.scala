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
abstract class LMDBStoreClusterUnitSuite extends JUnitSuite {

  def sc: SparkContext = LMDBStoreClusterUnitSuite.auxClass.sc

  def kvstoreMgr: KVStoreManager = LMDBStoreClusterUnitSuite.kvstoreMgr

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


object LMDBStoreClusterUnitSuite extends JUnitSuite {
  val ports: Array[Integer] = Array(45000, 45001, 45002).map(i => i: java.lang.Integer)
  //val ports: Array[Integer] = Array(45000).map(i => i: java.lang.Integer)
  class AuxClass extends FunSpec with SharedSparkContext {

    override def conf: SparkConf = {
      super.conf.set("spark.ui.enabled", "true")
    }
  }

  val backend="lmdb"
  var kvstoreMgr: KVStoreManager = _

  val auxClass = new AuxClass

  var servers: Array[KVStoreLocalhostServer] = null

  @BeforeClass
  def beforeClass(): Unit = {
    servers = ports.map {
      port =>
        new KVStoreLocalhostServer(port,backend)
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
