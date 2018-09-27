package org.jgi.spark.localcluster

/**
  * Created by Lizhen Shi on 9/27/18.
  */

import org.openjdk.jmh.annotations._

import scala.util.Random


@State(Scope.Benchmark)
class CKmerBenchmark {


  @Benchmark
  def kmer_gen(state: KmerBenchmark.MyState): Unit = {
    state.n = cKmer.generate_kmer(state.seq, 31).length
  }

}