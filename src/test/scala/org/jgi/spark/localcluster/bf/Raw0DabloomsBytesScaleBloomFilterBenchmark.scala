package org.jgi.spark.localcluster.bf

/**
  * Created by Lizhen Shi on 6/1/17.
  */
import com.github.jdablooms.ScaleBloomFilter
import org.openjdk.jmh.annotations.{Benchmark, Param, Scope, State}

import scala.util.Random
import com.github.jdablooms.cdablooms
@State(Scope.Benchmark)
class Raw0DabloomsBytesScaleBloomFilterBenchmark {

  private val itemsExpected = 100000000L
  private val falsePositiveRate = 0.01
  private val random = new Random()
  com.github.jdablooms.ScaleBloomFilter.load_native()
  private val bf = cdablooms.new_scaling_bloom(itemsExpected,falsePositiveRate,"tmp/Raw0DabloomsBytesScaleBloomFilterBenchmark.bin");

  var idx=0l
  @Param(Array("32"))
  var length: Int = _

  private val item = new Array[Byte](length)
  random.nextBytes(item)
  cdablooms.scaling_bloom_add_bytes(bf, item ,idx)
  idx+=1

  @Benchmark
  def myPut(): Unit = {
    cdablooms.scaling_bloom_add_bytes(bf, item ,idx)
    idx+=1
  }

  @Benchmark
  def myGet(): Unit = {
    cdablooms.scaling_bloom_check_bytes(bf,item)
  }

}

