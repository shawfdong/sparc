package org.jgi.spark.localcluster.bf

import bloomfilter.mutable.{BloomFilter=>ScalaBloomFilter}
import org.openjdk.jmh.annotations.{Benchmark, Param, Scope, State}

import scala.util.Random

@State(Scope.Benchmark)
class ScalaStringItemBenchmark {

  private val itemsExpected = 100000000L
  private val falsePositiveRate = 0.01
  private val random = new Random()

  private val bf =  ScalaBloomFilter[String](itemsExpected, falsePositiveRate)

  @Param(Array("32"))
  var length: Int = _

  private val item = random.nextString(length)
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