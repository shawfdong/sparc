package org.jgi.spark.localcluster.tools

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.SparkConf
import org.scalatest.{FlatSpec, Matchers, _}
import sext._

/**
  * Created by Lizhen Shi on 5/17/17.
  */
class GraphCC3Spec extends FlatSpec with Matchers with BeforeAndAfter with SharedSparkContext {

  override  def conf: SparkConf = super.conf.set("spark.ui.enabled", "true")


  "parse command line" should "be good" in {
    val cfg = GraphCC3.parse_command_line("-i test/graph_gen_test.txt -o tmp".split(" ")).get
    cfg.edge_file should be("test/graph_gen_test.txt")
  }
  "GraphCC3" should "work on the test seq files" in {
    val cfg = GraphCC3.parse_command_line(
      "-i test/graph_gen_test.txt   -o tmp/graph_cc3.txt --n_iteration 1 --min_reads_per_cluster 0".split(" ")
        .filter(_.nonEmpty)).get
    println(s"called with arguments\n${cfg.valueTreeString}")

    GraphCC3.run(cfg, sc)
    //Thread.sleep(1000*10000)
  }

  "GraphCC3" should "work on the test seq files with multiple iterations" in {
    val cfg = GraphCC3.parse_command_line(
      "-i test/graph_gen_test.txt   -o tmp/graph_cc3_iter3.txt --n_iteration 3 --min_reads_per_cluster 0".split(" ")
        .filter(_.nonEmpty)).get
    println(s"called with arguments\n${cfg.valueTreeString}")

    GraphCC3.run(cfg, sc)
   }
}
