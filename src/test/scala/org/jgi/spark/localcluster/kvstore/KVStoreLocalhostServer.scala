package org.jgi.spark.localcluster.kvstore

import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.ExecutionContext

/**
  * Created by Lizhen Shi on 6/2/17.
  */
class KVStoreLocalhostServer(val port: Int) extends Runnable with LazyLogging {
  private var server: KVStoreServer = null


  override def run() = {
    val backend: String = "lmdb"
    val bloomfilterName: String = "scala"
    val server = new KVStoreServer(port, ExecutionContext.global, backend, bloomfilterName)
    logger.info(s"start server at $port and backend=$backend, bloomfilter=$bloomfilterName")
    server.start()
    server.blockUntilShutdown()
  }

  def shutdown(): Unit = {
    logger.info(s"shutdown server at $port")
    if (server != null) server.stop()
  }
}
