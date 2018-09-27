package org.jgi.spark.localcluster.tools

import org.scalatest.{FlatSpec, Matchers, _}
import sext._

/**
  * Created by Lizhen Shi on 9/27/2018.
  */
class MakeSeqRocksdbSpec extends FlatSpec with Matchers with BeforeAndAfter {


  "parse command line" should "be good" in {
    val cfg = MakeSeqRocksdb.parse_command_line("--seq_file a.seq --db_file tmp/test.db".split(" ")).get
    cfg.seq_file should be("a.seq")
  }

  "MakeSeqRocksdb" should "work without compression" in {
    val cfg = MakeSeqRocksdb.parse_command_line("--seq_file data/sample.seq --db_file tmp/sample_no_compression.db".split(" ")
      .filter(_.nonEmpty)).get
    println(s"called with arguments\n${cfg.valueTreeString}")

    MakeSeqRocksdb.run(cfg)
  }

  "MakeSeqRocksdb" should "work compression" in {
    val cfg = MakeSeqRocksdb.parse_command_line("--compress --seq_file data/sample.seq --db_file tmp/sample_compression.db".split(" ")
      .filter(_.nonEmpty)).get
    println(s"called with arguments\n${cfg.valueTreeString}")

    MakeSeqRocksdb.run(cfg)
  }

}
