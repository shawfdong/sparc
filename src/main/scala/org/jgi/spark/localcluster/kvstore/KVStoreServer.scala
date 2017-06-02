package org.jgi.spark.localcluster.kvstore

import com.typesafe.scalalogging.LazyLogging
import io.grpc.{Server, ServerBuilder}

import scala.concurrent.ExecutionContext


/**
  * Created by Lizhen Shi on 6/1/17.
  */
object KVStoreServer extends LazyLogging {
  def main(args: Array[String]): Unit = {
    val backend: String = "lmdb"
    val bloomfilterName: String = "scala"
    val server = new KVStoreServer(port, ExecutionContext.global, backend, bloomfilterName)
    server.start()
    server.blockUntilShutdown()
  }

  private val port = 50051
}

class KVStoreServer(port:Int, executionContext: ExecutionContext, val backend: String, val bloomfilterName: String) extends LazyLogging {
  self =>
  private[this] var server: Server = null
  private  val service = new KVStoreServiceImpl(backend, bloomfilterName)

   def start(): Unit = {
    server = ServerBuilder.forPort(port).
      addService(KVStoreGrpc.bindService(service, executionContext)).build.start
    logger.info("Server started, listening on " + KVStoreServer.port)
    sys.addShutdownHook {
      System.err.println("*** shutting down gRPC server since JVM is shutting down")
      self.stop()
      System.err.println("*** server shut down")
    }
  }

    def stop(): Unit = {
    service.close()
    if (server != null) {
      server.shutdown()
    }
  }

    def blockUntilShutdown(): Unit = {
    if (server != null) {
      server.awaitTermination()
    }
  }
}
