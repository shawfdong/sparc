package org.jgi.spark.localcluster.tools

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{FlatSpec, Matchers, _}
import sext._

/**
  * Created by Lizhen Shi on 5/17/17.
  */
class GraphLPA3Spec extends FlatSpec with Matchers with BeforeAndAfter with DataFrameSuiteBase {

  "parse command line" should "be good" in {
    val cfg = GraphLPA3.parse_command_line("-i data/graph_gen_test.txt -o tmp".split(" ")).get
    cfg.edge_file should be("data/graph_gen_test.txt")
  }

  "GraphLPA3" should "work on the test seq files" in {

    val cfg = GraphLPA3.parse_command_line(
      "-i data/graph_gen_test.txt   -o tmp/graph_lpa3_graphx.txt --max_iteration 10  --min_reads_per_cluster 0".split(" ")
        .filter(_.nonEmpty)).get
    println(s"called with arguments\n${cfg.valueTreeString}")

    GraphLPA3.run(cfg, spark)
    //Thread.sleep(1000 * 10000)
  }

  "GraphLPA3" should "work with weight" in {

    val cfg = GraphLPA3.parse_command_line(
      "-i data/graph_gen_test.txt   -o tmp/graph_lpa3_graphx_weightedge.txt --max_iteration 10  --min_reads_per_cluster 0 --weight edge ".split(" ")
        .filter(_.nonEmpty)).get
    println(s"called with arguments\n${cfg.valueTreeString}")

    GraphLPA3.run(cfg, spark)
    //Thread.sleep(1000 * 10000)
  }
}
