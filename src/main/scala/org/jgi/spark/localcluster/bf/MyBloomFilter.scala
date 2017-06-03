package org.jgi.spark.localcluster.bf

import com.typesafe.scalalogging.LazyLogging

/**
  * Created by Lizhen Shi on 5/31/17.
  */
abstract class MyBloomFilter(val expectedElements: Long, val falsePositiveRate: Double) extends LazyLogging {

  def put(o: Array[Byte]): Unit

  def mightContain(o: Array[Byte]): Boolean

  def size(): Long

  def close(): Unit

  def mightContainIfNotThenPut(o: Array[Byte]): Boolean = {
    val contains = mightContain (o)
    if (!contains) put (o)
    contains
  }
}
