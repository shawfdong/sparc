package org.jgi.spark.localcluster.bf

import bloomfilter.mutable.BloomFilter
import org.openjdk.jmh.annotations.{Benchmark, Param, Scope, State}

import scala.util.Random

@State(Scope.Benchmark)
class ScalaArrayByteItemBenchmark {

  private val itemsExpected = 1000000L
  private val falsePositiveRate = 0.01
  private val random = new Random()

  private val bf = BloomFilter[Array[Byte]](itemsExpected, falsePositiveRate)

  @Param(Array("32"))
  var length: Int = _

  private val item = new Array[Byte](length)
  random.nextBytes(item)
  bf.add(item)

  @Benchmark
  def myPut(): Unit = {
    bf.add(item)
  }

  @Benchmark
  def myGet(): Unit = {
    bf.mightContain(item)
  }
}
