package org.jgi.spark.localcluster.bf

import java.util

import org.openjdk.jmh.annotations.{Benchmark, Param, Scope, State}

import scala.util.Random

/**
  * Created by Lizhen Shi on 5/31/17.
  */

@State(Scope.Benchmark)
class TestArrayWrapBenchmark {
  private val itemsExpected = 1000000L
  private val falsePositiveRate = 0.01
  private val random = new Random()

  private val bf = new ArrayWrap()
  private val bf2 = new util.HashSet[Array[Byte]]()
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

  @Benchmark
  def myPutRaw(): Unit = {
    bf2.add(item)
  }

  @Benchmark
  def myGetRaw(): Unit = {
    bf2.contains(item)
  }
}

class ArrayWrap {
  val array = new util.HashSet[Array[Byte]]()
  def put(a:Array[Byte]): Boolean = array.add(a)
  def mightContain(a:Array[Byte]): Boolean = array.contains(a)
}
