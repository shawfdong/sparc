package org.jgi.spark.localcluster

/**
  * Created by Lizhen Shi on 9/27/18.
  */

import org.openjdk.jmh.annotations._

import scala.util.Random


@State(Scope.Benchmark)
class OverlapBenchmark {


  @Benchmark
  def kmer_gen(state: OverlapBenchmark.MyState): Unit = {
    state.n = cKmer.sequence_overlap(state.seq1, state.seq2, 31, 0.00f)
  }

}

object OverlapBenchmark {

  @State(Scope.Thread)
  class MyState {
    def mkseq(n: Int): String = {
      (0 to n).map {
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

    @Setup(Level.Invocation)
    def doSetup(): Unit = {
      seq1 = mkseq(200);
      seq2 = mkseq(200);
    }


    var n: Int = 0;
    var seq1: String = null;
    var seq2: String = null;
  }

}