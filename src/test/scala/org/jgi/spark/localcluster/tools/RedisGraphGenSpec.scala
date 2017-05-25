package org.jgi.spark.localcluster.tools

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers, _}
import sext._

/**
  * Created by Lizhen Shi on 5/25/17.
  */
class RedisGraphGenSpec extends FlatSpec with Matchers with BeforeAndAfter with SharedSparkContext {
  private val master = "local[4]"
  private val appName = "GraphGenSpec"
  val REDIS_NODES = "127.0.0.1:42000,127.0.0.1:42001,127.0.0.1:42002"


  "parse command line" should "be good" in {
    val cfg = GraphGen.parse_command_line("-i test/kmermapping_test.txt -k 31 -o tmp".split(" ")).get
    cfg.kmer_reads should be("test/kmermapping_test.txt")
  }

  "graph gen" should "work on the test seq files" in {
    val cfg = GraphGen.parse_command_line(
      s"-i test/kmermapping_test.txt -k 31 -o tmp/redis_graph_gen_test.txt --n_iteration 1 --redis $REDIS_NODES".split(" ")
        .filter(_.nonEmpty)).get
    println(s"called with arguments\n${cfg.valueTreeString}")

    GraphGen.run(cfg, sc)
  }

  "graph gen" should "work on the test seq files for multiple iterations" in {
    val cfg = GraphGen.parse_command_line(
      s"-i test/kmermapping_test.txt -k 31 -o tmp/redis_graph_gen_test_iter3.txt --n_iteration 3 --redis $REDIS_NODES".split(" ")
        .filter(_.nonEmpty)).get
    println(s"called with arguments\n${cfg.valueTreeString}")

    GraphGen.run(cfg, sc)
  }
}
