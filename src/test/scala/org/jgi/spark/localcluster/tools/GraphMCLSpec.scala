package org.jgi.spark.localcluster.tools

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{FlatSpec, Matchers, _}
import sext._

/**
  * Created by Lizhen Shi on 5/17/17.
  */
class GraphMCLSpec extends FlatSpec with Matchers with BeforeAndAfter with DataFrameSuiteBase {

  "parse command line" should "be good" in {
    val cfg = GraphMCL.parse_command_line("-i data/graph_gen_test.txt -o tmp --matrix_block_size 4".split(" ")).get
    cfg.edge_file should be("data/graph_gen_test.txt")
  }

  "GraphMCL" should "work on the test seq files" in {

    val cfg = GraphMCL.parse_command_line(
      "-i data/graph_gen_test.txt   -o tmp/graph_ap.txt --max_iteration 10  --min_reads_per_cluster 0 --scaling 1 --matrix_block_size 4".split(" ")
        .filter(_.nonEmpty)).get
    println(s"called with arguments\n${cfg.valueTreeString}")

    GraphMCL.run(cfg, spark)
    //Thread.sleep(1000 * 10000)
  }

  "GraphMCL" should "work with weight" in {

    val cfg = GraphMCL.parse_command_line(
      "-i data/graph_gen_test.txt   -o tmp/graph_ap_weightedge.txt --max_iteration 10  --min_reads_per_cluster 0 --weight edge  --scaling 0.5 ".split(" ")
        .filter(_.nonEmpty)).get
    println(s"called with arguments\n${cfg.valueTreeString}")

    GraphMCL.run(cfg, spark)
    //Thread.sleep(1000 * 10000)
  }
}
