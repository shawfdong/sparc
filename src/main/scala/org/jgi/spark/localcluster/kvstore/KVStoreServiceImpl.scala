package org.jgi.spark.localcluster.kvstore

import java.util
import java.util.List

import org.jgi.spark.localcluster.bf.{DabloomsScaleBloomFilter, MyBloomFilter, ScalaScaleBloomFilter}

import scala.collection.JavaConversions._

import scala.concurrent.Future


/**
  * Created by Lizhen Shi on 6/2/17.
  */
class KVStoreServiceImpl(val backendName: String, val bloomfilterName: String) extends KVStoreGrpc.KVStore {

  private var bloomFilter: MyBloomFilter = null
  protected val backend =
    if ("lmdb".equals(backendName)) {
      new LMDBBackend()
    } else {
      throw new IllegalAccessException("invalid backend name: " + backendName);
    }

  def this(backendName: String) = this(backendName, null)


  override def incKmerInBatch(request: KmerBatchRequest): Future[StatusReply] = {
    val kmers = request.kmers
    val use_bloomfilter: Boolean = request.useBloomFilter
    var reply:StatusReply = null
    try {
      val filteredKmers =
        if (use_bloomfilter) {
          val bf = this.getBloomFilter
          kmers.filter(x => bf.mightContain(x.toByteArray))
        } else
          kmers

      if (use_bloomfilter) {
        backend.incr(filteredKmers)
      } else {
        backend.incr(filteredKmers)
      }
      reply = StatusReply(status = StatusReply.StatusType.Success)
    }
    catch {
      case e: Exception =>
        reply = StatusReply(status = StatusReply.StatusType.Failure, message = e.getMessage)

    }
    Future.successful(reply)
  }

  override def getKmerCounts(request: NullRequest): Future[KmerCountReply] =  {

    val array = backend.getKmerCounts
    val status =Some(StatusReply(status = StatusReply.StatusType.Success))
    val reply = KmerCountReply(status= status ,batch=array)
    Future.successful(reply)
  }

  override def incEdgeInBatch(request: EdgeBatchRequest): Future[StatusReply] = ???

  override def getEdgeCounts(request: NullRequest): Future[EdgeCountReply] = ???

  override def flush(request: NullRequest): Future[StatusReply] = ???


  @throws[java.lang.Exception]
  def getBloomFilter: MyBloomFilter = {
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
}
