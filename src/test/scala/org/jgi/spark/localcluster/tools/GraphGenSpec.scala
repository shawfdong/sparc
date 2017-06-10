package org.jgi.spark.localcluster.tools

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers, _}
import sext._

/**
  * Created by Lizhen Shi on 5/17/17.
  */
class GraphGenSpec extends FlatSpec with Matchers with BeforeAndAfter with SharedSparkContext {
  private val master = "local[4]"
  private val appName = "GraphGenSpec"


  "parse command line" should "be good" in {
    val cfg = GraphGen.parse_command_line("-i test/kmermapping_test.txt -k 31 -o tmp".split(" ")).get
    cfg.kmer_reads should be("test/kmermapping_test.txt")
  }

  "graph gen" should "work on the test seq files" in {
    val cfg = GraphGen.parse_command_line(
      "-i test/kmermapping_test.txt -k 31 -o tmp/graph_gen_test.txt --n_iteration 1".split(" ")
        .filter(_.nonEmpty)).get
    println(s"called with arguments\n${cfg.valueTreeString}")

    GraphGen.run(cfg, sc)
  }

  "graph gen" should "work on the test seq files for multiple iterations" in {
    val cfg = GraphGen.parse_command_line(
      "-i test/kmermapping_test.txt -k 31 -o tmp/graph_gen_test_iter3.txt --n_iteration 3".split(" ")
        .filter(_.nonEmpty)).get
    println(s"called with arguments\n${cfg.valueTreeString}")

    GraphGen.run(cfg, sc)
  }



  "graph gen" should "work on the fake seq file" in {
    val cfg = GraphGen.parse_command_line(
      "-i test/kmermapping_seq_fake.txt -k 31 -o tmp/graph_gen_fake.txt --n_iteration 1".split(" ")
        .filter(_.nonEmpty)).get
    println(s"called with arguments\n${cfg.valueTreeString}")

    GraphGen.run(cfg, sc)
  }
}
