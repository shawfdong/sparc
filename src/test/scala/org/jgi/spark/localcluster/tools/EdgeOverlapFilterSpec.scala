package org.jgi.spark.localcluster.tools

import java.nio.file.{Files, Paths}

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{FlatSpec, Matchers, _}
import sext._

/**
  * Created by Lizhen Shi on 9/27/2018.
  */
class EdgeOverlapFilterSpec extends FlatSpec with Matchers with BeforeAndAfter with DataFrameSuiteBase {

  "parse command line" should "be good" in {
    val cfg = EdgeOverlapFilter.parse_command_line("-i data/graph_gen_test.txt -o tmp --db x".split(" ")).get
    cfg.edge_file should be("data/graph_gen_test.txt")
  }

  def prepare_db() = {
    if (!Files.exists(Paths.get("tmp/sample_test.db"))) {
      val cfg = MakeSeqRocksdb.parse_command_line("--seq_file data/sample.seq --db_file tmp/sample_test.db".split(" ")
        .filter(_.nonEmpty)).get
      println(s"called with arguments\n${cfg.valueTreeString}")

      MakeSeqRocksdb.run(cfg)
    }
  }

  "EdgeOverlapFilter" should "work on the test seq files" in {
    prepare_db()
    val cfg = EdgeOverlapFilter.parse_command_line(
      "--db tmp/sample_test.db -i data/graph_gen_test.txt -o tmp/graph_gen_test.filtered.txt --min_reads_per_cluster 0 --min_overlap 11".split(" ")
        .filter(_.nonEmpty)).get
    println(s"called with arguments\n${
      cfg.valueTreeString
    }")

    EdgeOverlapFilter.run(cfg, spark)
    //Thread.sleep(1000 * 10000)
  }

}
