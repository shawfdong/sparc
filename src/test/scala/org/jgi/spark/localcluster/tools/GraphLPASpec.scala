package org.jgi.spark.localcluster.tools

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{FlatSpec, Matchers, _}
import sext._

/**
  * Created by Lizhen Shi on 5/17/17.
  */
class GraphLPASpec extends FlatSpec with Matchers with BeforeAndAfter with DataFrameSuiteBase {

  "parse command line" should "be good" in {
    val cfg = GraphLPA.parse_command_line("-i test/graph_gen_test.txt -o tmp".split(" ")).get
    cfg.edge_file should be("test/graph_gen_test.txt")
  }

  "GraphLPA" should "work on the test seq files" in {
    val cfg = GraphLPA.parse_command_line(
      "-i test/graph_gen_test.txt   -o tmp/graph_lpa.txt --max_iteration 10  --min_reads_per_cluster 0".split(" ")
        .filter(_.nonEmpty)).get
    println(s"called with arguments\n${cfg.valueTreeString}")

    GraphLPA.run(cfg, spark)
    //Thread.sleep(1000 * 10000)
  }

}
