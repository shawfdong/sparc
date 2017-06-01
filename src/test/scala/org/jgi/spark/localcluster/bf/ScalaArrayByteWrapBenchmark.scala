package org.jgi.spark.localcluster.bf

import org.openjdk.jmh.annotations.{Benchmark, Param, Scope, State}

import scala.util.Random
import org.jgi.spark.localcluster.BloomFilterBytes

@State(Scope.Benchmark)
class ScalaArrayByteWrapBenchmark {

  private val itemsExpected = 1000000L
  private val falsePositiveRate = 0.01
  private val random = new Random()

  private val bf = new  BloomFilterBytes(itemsExpected, falsePositiveRate)

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
