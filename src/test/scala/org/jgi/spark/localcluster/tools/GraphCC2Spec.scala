package org.jgi.spark.localcluster.tools

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.SparkConf
import org.scalatest.{FlatSpec, Matchers, _}
import sext._

/**
  * Created by Lizhen Shi on 5/17/17.
  */
class GraphCC2Spec extends FlatSpec with Matchers with BeforeAndAfter with SharedSparkContext {

  override  def conf: SparkConf = super.conf.set("spark.ui.enabled", "true")


  "parse command line" should "be good" in {
    val cfg = GraphCC2.parse_command_line("-i test/graph_gen_test.txt -o tmp".split(" ")).get
    cfg.edge_file should be("test/graph_gen_test.txt")
  }
  "GraphCC2" should "work on the test seq files" in {
    val cfg = GraphCC2.parse_command_line(
      "-i test/graph_gen_test.txt   -o tmp/graph_cc2.txt --n_iteration 1 --min_reads_per_cluster 0".split(" ")
        .filter(_.nonEmpty)).get
    println(s"called with arguments\n${cfg.valueTreeString}")

    GraphCC2.run(cfg, sc)
    Thread.sleep(1000*10000)
  }

  "GraphCC2" should "work on the test seq files with multiple iterations" in {
    val cfg = GraphCC2.parse_command_line(
      "-i test/graph_gen_test.txt   -o tmp/graph_cc2_iter3.txt --n_iteration 3 --min_reads_per_cluster 0".split(" ")
        .filter(_.nonEmpty)).get
    println(s"called with arguments\n${cfg.valueTreeString}")

    GraphCC2.run(cfg, sc)
   }
}
