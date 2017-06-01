package org.jgi.spark.localcluster.bf

/**
  * Created by Lizhen Shi on 6/1/17.
  */
import com.github.jdablooms.ScaleBloomFilter
import org.openjdk.jmh.annotations.{Benchmark, Param, Scope, State}

import scala.util.Random

@State(Scope.Benchmark)
class RawDabloomsBytesScaleBloomFilterBenchmark {

  private val itemsExpected = 100000000L
  private val falsePositiveRate = 0.01
  private val random = new Random()

  private val bf = new ScaleBloomFilter(itemsExpected,falsePositiveRate);

  @Param(Array("32"))
  var length: Int = _

  private val item = new Array[Byte](length)
  random.nextBytes(item)
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

