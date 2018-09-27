package org.jgi.spark.localcluster

/**
  * Created by Lizhen Shi on 9/27/18.
  */

import org.openjdk.jmh.annotations._

import scala.util.Random


@State(Scope.Benchmark)
class KmerBenchmark {


  @Benchmark
  def kmer_gen(state: KmerBenchmark.MyState): Unit = {
    state.n = Kmer.generate_kmer(state.seq, 31).length
  }

}

object KmerBenchmark {

  @State(Scope.Thread)
  class MyState {
    @Setup(Level.Invocation)
    def doSetup(): Unit = {
      seq = (0 to 1000).map {
        _ =>
          val i = Random.nextInt() % 4
          if (i > 0) i else i + 4
      }.map {
        i =>
          if (i == 0)
            'A'
          else if (i == 1)
            'G'
          else if (i == 2)
            'C'
          else
            'T'
      }.mkString("")
    }


    var n: Int = 0;
    var seq: String = null;
  }

}