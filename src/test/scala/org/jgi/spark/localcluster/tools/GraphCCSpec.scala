package org.jgi.spark.localcluster.tools

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers, _}
import sext._

/**
  * Created by Lizhen Shi on 5/17/17.
  */
class GraphCCSpec extends FlatSpec with Matchers with BeforeAndAfter with SharedSparkContext {
  private val master = "local[4]"
  private val appName = "GraphCCSpec"


  "parse command line" should "be good" in {
    val cfg = GraphCC.parse_command_line("-i test/graph_gen_test.txt -o tmp".split(" ")).get
    cfg.edge_file should be("test/graph_gen_test.txt")
  }
  "GraphCC" should "work on the test seq files" in {
    val cfg = GraphCC.parse_command_line(
      "-i test/graph_gen_test.txt   -o tmp/graphx_cc.txt --n_iteration 3 --min_reads_per_cluster 0".split(" ")
        .filter(_.nonEmpty)).get
    println(s"called with arguments\n${cfg.valueTreeString}")

    GraphCC.run(cfg, sc)
    //Thread.sleep(1000 * 10000)
  }

  "GraphCC" should "work on the fake seq file" in {
    val cfg = GraphCC.parse_command_line(
      "-i test/graph_gen_fake.txt   -o tmp/graphx_cc_fake.txt --n_iteration 1 --min_reads_per_cluster 0".split(" ")
        .filter(_.nonEmpty)).get
    println(s"called with arguments\n${cfg.valueTreeString}")

    GraphCC.run(cfg, sc) should be (100)
    //Thread.sleep(1000 * 10000)
  }
}
