package org.jgi.spark.localcluster.tools

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.SparkConf
import org.scalatest.{FlatSpec, Matchers, _}
import sext._

/**
  * Created by Lizhen Shi on 6/8/17.
  */
class CCAddSeqSpec extends FlatSpec with Matchers with BeforeAndAfter with SharedSparkContext {

  override def conf: SparkConf = super.conf.set("spark.ui.enabled", "true")


  "parse command line" should "be good" in {
    val cfg = CCAddSeq.parse_command_line("--reads data -p *.seq -i data/cc_test.txt -o tmp/cc_seq.txt".split(" ")).get
    cfg.cc_file should be("data/cc_test.txt")
  }

  "CCAddSeqSpec" should "work on the test seq and cc files" in {
    val cfg = CCAddSeq.parse_command_line(
      "--reads data -p *.seq -i data/cc_test.txt -o tmp/cc_seq.txt".split(" ")
        .filter(_.nonEmpty)).get
    println(s"called with arguments\n${cfg.valueTreeString}")

    CCAddSeq.run(cfg, sc)
  }


}
