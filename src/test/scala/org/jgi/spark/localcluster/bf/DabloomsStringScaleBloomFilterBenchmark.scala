package org.jgi.spark.localcluster.bf

import org.openjdk.jmh.annotations.{Benchmark, Param, Scope, State}

import scala.util.Random

/**
  * Created by Lizhen Shi on 5/31/17.
  */

@State(Scope.Benchmark)
class DabloomsStringScaleBloomFilterBenchmark {
  private val itemsExpected = 1000000L
  private val falsePositiveRate = 0.01
  private val random = new Random()

  private val bf = new DabloomsScaleBloomFilter(itemsExpected,falsePositiveRate)

  @Param(Array("32"))
  var length: Int = _

  private val item = random.nextString(length)
  bf.put(item)

  @Benchmark
  def myPut(): Unit = {
    bf.put(item)
  }

  @Benchmark
  def myGet(): Unit = {
    bf.mightContain(item)
  }
}
