package org.jgi.spark.localcluster.tools

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.jgi.spark.localcluster.tools.Seq2Parquet.APPNAME
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import sext._

/**
  * Created by Lizhen Shi on 5/15/17.
  */
class Seq2Base64Spec extends FlatSpec with Matchers with BeforeAndAfter {
  private val master = "local[4]"
  private val appName = "Seq2ParquetSpec"
  //private val checkpointDir = Files.createTempDirectory(appName).toString

  private var spark: SparkSession = _

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .set("spark.executor.memory", "8G")
      .set("spark.hadoop.validateOutputSpecs", "false")
      .setAppName(appName)

    spark = SparkSession
      .builder()
      .appName(APPNAME)
      .config(conf)
      .getOrCreate()
  }

  after {
    if (spark != null) {
      spark.stop()
      spark=null
    }
  }

  "parse command line" should "be good" in {
    val cfg = Seq2Base64.parse_command_line("-i test -p *.seq -o tmp".split(" ")).get
    cfg.input should be ("test")
  }
  "Seq2Base64" should "work on the test seq files" in {
    val cfg = Seq2Base64.parse_command_line("-i test -p *.seq -o tmp/test.base64 -n 4".split(" ")).get
    print(s"called with arguments\n${cfg.valueTreeString}")


    Seq2Base64.run(cfg, spark)

    //Thread.sleep(1000 * 10000)
  }
}
