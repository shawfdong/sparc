package org.jgi.spark.localcluster.kvstore

import com.typesafe.scalalogging.LazyLogging
import org.jgi.spark.localcluster.DNASeq
import scala.collection.mutable._
import scala.util.Random

/**
  * Created by Lizhen Shi on 6/2/17.
  */
class KVStoreManager(val kvstoreSlots: Array[KVStoreSlot]) extends LazyLogging {

  val _hostsAndPorts = kvstoreSlots.map(x => (x.ip, x.port)).distinct

  def this(hostsAndPortsSet: collection.immutable.Set[(String, Int)]) = {
    this {
      hostsAndPortsSet.toArray.sorted.map(x   => KVStoreSlot(x._1,x._2))
    }
  }


  val rand = new Random(System.currentTimeMillis())

  private def get_pool(slot: KVStoreSlot): Option[KVStorePool] = {
    get_pool(slot.ip, slot.port)
  }

  private def get_pool(ip: String, port: Int): Option[KVStorePool] = {
    val ipAndPort = (ip, port)
    if (!kvstore_pool_ins_map.contains(ipAndPort)) {

      val poolConfig = new KVStorePoolConfig()
      poolConfig.setMaxTotal(256); // maximum active connections
	    poolConfig.setMaxWaitMillis(30*1000);
      kvstore_pool_ins_map.put(ipAndPort, new KVStorePool(poolConfig, ip, port,30*1000))
    }
    kvstore_pool_ins_map.get(ipAndPort)
  }

  def getKVStoreClient(ip: String, port: Int): KVStoreClient = {
    get_pool(ip, port) match {
      case Some(pool) => pool.getResource
      case None => throw new Exception("never be here")
    }
  }

  def getSlot(keyHash: Int): KVStoreSlot = {
    kvstoreSlots(keyHash % kvstoreSlots.length)
  }


  def getKVStoreClient(slot: KVStoreSlot): KVStoreClient = {
    get_pool(slot) match {
      case Some(pool) => pool.getResource
      case None => throw new Exception("never be here")
    }
  }

  def flushAll(): Unit = {
    _hostsAndPorts.indices.foreach {
      i =>
        val ip = _hostsAndPorts(i)._1
        val port = _hostsAndPorts(i)._2
        val obj = getKVStoreClient(ip,port )

          var code = obj.flushAll()
          logger.debug(s"flush node $ip, $port, response $code")

          if (!"OK".equals(code)) {
            code = obj.flushAll()
            logger.debug(s"flush node $ip, $port, response $code")
          }

        obj.close()
    }
  }


  @transient val kvstore_pool_ins_map: HashMap[(String, Int), KVStorePool] = new HashMap[(String, Int), KVStorePool]()

  def close(): Unit = {
    if (kvstore_pool_ins_map.nonEmpty) {
      logger.info("KVStoreManager: jvm exiting, destroying kvstore pool")
      kvstore_pool_ins_map.values.foreach(_.destroy())
    }
  }

  Runtime.getRuntime.addShutdownHook(new Thread(new Runnable() {
    def run(): Unit = {
      close()
    }
  }))

  logger.info("create KVstore manager with slots:\n" + kvstoreSlots.map(_.toString).mkString("\n")
  )

}

object KVstoreManagerSingleton extends LazyLogging {

  @transient private var _instance: KVStoreManager = null

  def instance(kvstore_ip_ports: Array[(String, Int)]): KVStoreManager = {
    if (_instance == null) {

      _instance = new KVStoreManager(kvstore_ip_ports.toSet)
      logger.info("create singlton instance for the first time.")
      logger.info(_instance.kvstoreSlots.map(x => x.toString).mkString("\n"))

    }
    _instance
  }

  def instance: KVStoreManager = _instance

  def instance(kvstoreSlots: Array[KVStoreSlot]): KVStoreManager = {
    if (_instance == null) {
      logger.info("create singlton instance for the first time.")
      logger.info(kvstoreSlots.map(x => x.toString).mkString("\n"))

      _instance = new KVStoreManager(kvstoreSlots)
    }
    _instance
  }
}
