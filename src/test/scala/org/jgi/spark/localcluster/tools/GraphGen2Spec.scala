package org.jgi.spark.localcluster.tools

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers, _}
import sext._

/**
  * Created by Lizhen Shi on 5/17/17.
  */
class GraphGen2Spec extends FlatSpec with Matchers with BeforeAndAfter with SharedSparkContext {

  "parse command line" should "be good" in {
    val cfg = GraphGen2.parse_command_line("-i data/kmermapping_test.txt -o tmp".split(" ")).get
    cfg.kmer_reads should be("data/kmermapping_test.txt")
  }

  "graph gen" should "work on the test seq files" in {
    val cfg = GraphGen2.parse_command_line(
      "-i data/kmermapping_test.txt -o tmp/graph_gen2_test.txt --n_iteration 1".split(" ")
        .filter(_.nonEmpty)).get
    println(s"called with arguments\n${cfg.valueTreeString}")

    GraphGen2.run(cfg, sc)
  }

  "graph gen" should "work on the test seq files for multiple iterations" in {
    val cfg = GraphGen2.parse_command_line(
      "-i data/kmermapping_test.txt -o tmp/graph_gen2_test_iter3.txt --n_iteration 3".split(" ")
        .filter(_.nonEmpty)).get
    println(s"called with arguments\n${cfg.valueTreeString}")

    GraphGen2.run(cfg, sc)
  }



  "graph gen" should "work on the fake seq file" in {
    val cfg = GraphGen2.parse_command_line(
      "-i data/kmermapping_seq_fake.txt -o tmp/graph_gen_fake.txt --n_iteration 1".split(" ")
        .filter(_.nonEmpty)).get
    println(s"called with arguments\n${cfg.valueTreeString}")

    GraphGen2.run(cfg, sc)
  }
}
