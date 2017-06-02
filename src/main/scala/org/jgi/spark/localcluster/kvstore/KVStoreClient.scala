package org.jgi.spark.localcluster.kvstore

import java.util.concurrent.TimeUnit

import com.google.protobuf.ByteString
import com.typesafe.scalalogging.LazyLogging
import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import org.jgi.spark.localcluster.DNASeq

/**
  * Created by Lizhen Shi on 6/2/17.
  */
object KVStoreClient extends LazyLogging {

  def apply(host: String, port: Int, connectionTimeout: Int, soTimeout: Int): KVStoreClient = {
    val channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext(true).build
    val blockingStub = KVStoreGrpc.blockingStub(channel)
    new KVStoreClient(host, port, channel, blockingStub, connectionTimeout, soTimeout)
  }

  def apply(host: String, port: Int): KVStoreClient = {
    val channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext(true).build
    val blockingStub = KVStoreGrpc.blockingStub(channel)
    new KVStoreClient(host, port, channel, blockingStub, 20000, 20000)
  }


  def main(args: Array[String]): Unit = {
    val client = KVStoreClient("localhost", 50051)
    try {

    } finally {
      client.shutdown()
    }
  }
}

class KVStoreClient private(
                             val host: String, val port: Int,
                             private val channel: ManagedChannel,
                             private val blockingStub: KVStoreGrpc.KVStoreBlockingStub,
                             connectionTimeout: Int, soTimeout: Int
                           ) extends LazyLogging {

  private var is_connected = true
  private var is_broken = false
  private var client_name = "null"
  protected var dataSource: Pool[KVStoreClient] = null


  def shutdown(): Unit = {
    channel.shutdown.awaitTermination(5, TimeUnit.SECONDS)
    is_connected = false
  }

  def isConnected(): Boolean = is_connected

  def isBroken: Boolean = is_broken

  //ask server to close connection
  def quit() = {}

  def connect() = {}

  def close() = {
    if (dataSource != null) {
      if (isBroken) {
        this.dataSource.returnBrokenResource(this);
      } else {
        this.dataSource.returnResource(this);
      }
    } else {
      disconnect()
    }
  }

  def clientSetname(name: String) = {
    client_name = name
  }

  //close socket from server
  def disconnect(): Unit = shutdown()

  def setDataSource(kvstorePool: Pool[KVStoreClient]): Unit = {
    this.dataSource = kvstorePool
  }


  /*                     business methods           */
  def incr_kmer_in_batch(kmers: Array[DNASeq], useBloomFilter: Boolean) = {
    val request = KmerBatchRequest(kmers = kmers.map(x => ByteString.copyFrom(x.bytes)), useBloomFilter = useBloomFilter)
    val response = blockingStub.incrKmerInBatch(request = request)
  }

  def get_kmer_counts(useBloomFilter: Boolean, minimumCount: Int): Seq[(DNASeq, Int)] = {
    val response = blockingStub.getKmerCounts(KmerCountRequest(useBloomFilter = useBloomFilter, minimumCount = minimumCount))
    response.batch.map(x => (new DNASeq(x.kmer.toByteArray), x.count))
  }

  def flush():String = {
    blockingStub.flush(Empty.defaultInstance).reply
  }

  def flushAll():String = flush()

  def ping(): String = {
    blockingStub.ping(Empty.defaultInstance).reply
  }


}
