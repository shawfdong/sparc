package org.jgi.spark.localcluster.kvstore

import org.jgi.spark.localcluster.bf.{DabloomsScaleBloomFilter, MyBloomFilter, ScalaScaleBloomFilter}

import scala.collection.JavaConversions._
import scala.concurrent.Future


/**
  * Created by Lizhen Shi on 6/2/17.
  */
class KVStoreServiceImpl(val backendName: String, val bloomfilterName: String) extends KVStoreGrpc.KVStore {

  private var bloomFilter: MyBloomFilter = null
  protected val backend: Backend =
    if ("lmdb".equals(backendName)) {
      new LMDBBackend()
    } else {
      throw new IllegalAccessException("invalid backend name: " + backendName)
    }

  def this(backendName: String) = this(backendName, null)


  override def incrKmerInBatch(request: KmerBatchRequest): Future[Empty] = {
    val kmers = request.kmers
    val use_bloomfilter: Boolean = request.useBloomFilter

    val filteredKmers =
      if (use_bloomfilter) {
        val bf = this.getBloomFilter
        kmers.filter(x => bf.mightContainIfNotThenPut(x.toByteArray))
      } else
        kmers

    backend.incr(filteredKmers)


    Future.successful(Empty.defaultInstance)
  }

  override def getKmerCounts(request: KmerCountRequest): Future[KmerCountReply] = {

    val array = backend.getKmerCounts(request.useBloomFilter, request.minimumCount)
    val reply = KmerCountReply(batch = array)
    Future.successful(reply)
  }

  override def incrEdgeInBatch(request: EdgeBatchRequest): Future[Empty] = ???

  override def getEdgeCounts(request: EdgeCountRequest): Future[EdgeCountReply] = ???

  override def flush(request: Empty): Future[FlushReply] = {
    close()
    Future.successful(FlushReply("OK"))
  }

  def close(): Unit = this.synchronized {
    backend.delete()
    if (bloomFilter != null) {
      bloomFilter.close
      bloomFilter = null
    }
  }


  @throws[java.lang.Exception]
  def getBloomFilter: MyBloomFilter = this.synchronized {
    if (bloomFilter == null) bloomFilter = createBloomFilter(bloomfilterName)
    bloomFilter
  }

  @throws[java.lang.Exception]
  private def createBloomFilter(bloomfilterName: String): MyBloomFilter = {
    val expectedElements = 64 * 1024 * 1024
    val falsePositiveRate = 0.01
    if ("scala" == bloomfilterName) new ScalaScaleBloomFilter(expectedElements, falsePositiveRate)
    else if ("dablooms" == bloomfilterName) new DabloomsScaleBloomFilter(expectedElements, falsePositiveRate)
    else throw new Exception("Unknow bloomfilter: " + bloomfilterName)
  }

  override def ping(request: Empty): Future[PingReply] = Future.successful(PingReply("PONG"))
}
