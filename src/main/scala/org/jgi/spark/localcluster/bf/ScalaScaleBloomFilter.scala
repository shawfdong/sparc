package org.jgi.spark.localcluster.bf

import java.util.Random

import breeze.util.BloomFilter
import org.jgi.spark.localcluster.GuavaBytesBloomFilter

/**
  * Created by Lizhen Shi on 5/31/17.
  */
class ScalaScaleBloomFilter(expectedElements: Long, falsePositiveRate: Double) extends MyBloomFilter(expectedElements, falsePositiveRate) {

  class InternalBloomFilter(expectedElements: Long, falsePositiveRate: Double) extends MyBloomFilter(expectedElements, falsePositiveRate) {
    //println("AAAAAAAAA", expectedElements, falsePositiveRate)
    private val filter = bloomfilter.mutable.BloomFilter[Array[Byte]](expectedElements, falsePositiveRate)
    private var count: Long = 0

    override def put(o: Array[Byte]): Unit = {
      count += 1
      filter.add(o)
    }

    val capacity = expectedElements

    override def mightContain(o: Array[Byte]): Boolean = filter.mightContain(o)

    override def size: Long = count

    override def close(): Unit = filter.dispose()
  }

  private val ERROR_TIGHTENING_RATIO = 0.8
  private val filters = new java.util.ArrayList[InternalBloomFilter]
  private var curr_idx: Long = 0
  private var curr_filter: InternalBloomFilter = null


  private def create_new_filter(): InternalBloomFilter = {
    val error_rate = falsePositiveRate * math.pow(ERROR_TIGHTENING_RATIO, num_blooms)
    val filter = new InternalBloomFilter(expectedElements, error_rate)
    filters.add(filter)
    filter
  }

  def num_blooms = filters.size()

  def put(o: Array[Byte]): Unit = {
    if (curr_filter == null) {
      curr_filter = create_new_filter()
      curr_idx = 0
    }
    else if (curr_idx > curr_filter.capacity) {
      curr_filter = create_new_filter()
      curr_idx = 0
    }

    curr_filter.put(o)
    curr_idx += 1
  }

  def mightContain(o: Array[Byte]): Boolean = {
    for (i <- 0.until(filters.size())) {
      if (filters.get(i).mightContain(o)) {
        return true
      }
    }
    return false
  }

  def size(): Long = {
    (0 until filters.size()).map(filters.get(_)).map(_.size).sum
  }

  override def close: Unit = {
    (0 until filters.size()).map(filters.get(_)).foreach(_.close())
  }
}

object ScalaScaleBloomFilter {
  def main(argv: Array[String]): Unit = {
    val itemsExpected = 1000000L
    val falsePositiveRate = 0.01
    val random = new Random
    val bf = new ScalaScaleBloomFilter(itemsExpected, falsePositiveRate)
    val bytes = new Array[Byte](32)
    random.nextBytes(bytes)
    val N=1000 * 1000 * 10

    var t0 = System.currentTimeMillis

    0.until(N).foreach {
      _ =>
        random.nextBytes(bytes)
        bf.put(bytes)
    }

    var dt:Double  = N/(-1 * (t0 - System.currentTimeMillis) / 1000.0)
    println(dt)

    val bf2 = bloomfilter.mutable.BloomFilter[Array[Byte]](itemsExpected, falsePositiveRate)
    t0 = System.currentTimeMillis
    0.until(N).foreach {
      _ =>
        random.nextBytes(bytes)
        bf2.add(bytes)
    }

    dt = N/(-1 * (t0 - System.currentTimeMillis) / 1000.0)
    println(dt)

    val bf3 = new  GuavaBytesBloomFilter(itemsExpected.toInt, falsePositiveRate)
    t0 = System.currentTimeMillis
    0.until(N).foreach {
      _ =>
        random.nextBytes(bytes)
        bf3.add(bytes)
    }
    dt = N/(-1 * (t0 - System.currentTimeMillis) / 1000.0)
    println(dt)



    val bf4 = BloomFilter.optimallySized[Array[Byte]](itemsExpected.toInt, falsePositiveRate)
    t0 = System.currentTimeMillis
    0.until(N).foreach {
      _ =>
        random.nextBytes(bytes)
        bf4 += bytes
    }
    dt = N/(-1 * (t0 - System.currentTimeMillis) / 1000.0)
    println(dt)

  }
}
