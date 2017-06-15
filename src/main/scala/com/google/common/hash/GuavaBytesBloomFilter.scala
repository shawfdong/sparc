package com.google.common.hash

import com.google.common.hash.BloomFilterStrategies.BitArray
import GuavaBloomFilterStrategies.GuavaBitArray
import org.jgi.spark.localcluster.AbstractBloomFilter

/**
  * Created by Lizhen Shi on 6/15/17.
  */

@SerialVersionUID(789L)
class GuavaBytesBloomFilter(expectedElements: Long, falsePositiveRate: Double)
  extends AbstractBloomFilter[Array[Byte]](expectedElements, falsePositiveRate) {

  private var underlying = create_bloom_filter()

  def +=(bytes: Array[Byte]) = {
    underlying.put(bytes)
  }

  def |(other: GuavaBytesBloomFilter) = {
    val a = other.underlying.bits.data
    val b = this.underlying.bits.data
    require(a.length == b.length)
    val bitarray = new GuavaBitArray(a.zip(b).map(u => u._1 | u._2))
    this.underlying = new GuavaBloomFilter[Array[Byte]](bitarray, numHashFunctions,
      Funnels.byteArrayFunnel(), GuavaBloomFilterStrategies.MURMUR128_MITZ_64)
    this
  }


  private var numBits = 0l

  private var numHashFunctions = 0

  private def create_bloom_filter(): GuavaBloomFilter[Array[Byte]] = {
    val funnel = Funnels.byteArrayFunnel()
    require(expectedElements > 0, "Expected insertions (%s) must be >  0")
    require(falsePositiveRate > 0.0, "False positive probability (%s) must be > 0.0")
    require(falsePositiveRate < 1.0, "False positive probability (%s) must be < 1.0")

    val strategy = GuavaBloomFilterStrategies.MURMUR128_MITZ_64

    numBits = BloomFilter.optimalNumOfBits(expectedElements, falsePositiveRate)
    numHashFunctions = BloomFilter.optimalNumOfHashFunctions(expectedElements, numBits)
    try {

      val bitarray = new GuavaBitArray(numBits)
      return new GuavaBloomFilter[Array[Byte]](bitarray, numHashFunctions, funnel, strategy)
    } catch {
      case e: IllegalArgumentException =>
        throw new IllegalArgumentException("Could not create BloomFilter of " + numBits + " bits", e)
    }
  }

  override def mightContain(o: Array[Byte]): Boolean = underlying.mightContain(o)

  override def _add(x: Array[Byte]): Unit = underlying.put(x)
}

