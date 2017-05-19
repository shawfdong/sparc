package org.jgi.spark.localcluster.tools

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.jgi.spark.localcluster.tools.Seq2Parquet.APPNAME
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import sext._
/**
  * Created by Lizhen Shi on 5/15/17.
  */
class Seq2ParquetSpec extends FlatSpec with Matchers with BeforeAndAfter {
  private val master = "local[4]"
  private val appName = "Seq2ParquetSpec"
  //private val checkpointDir = Files.createTempDirectory(appName).toString

  private var spark: SparkSession = _

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .set("spark.executor.memory", "8G")
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
    val cfg = Seq2Parquet.parse_command_line("-i test -p *.seq -o tmp".split(" ")).get
    cfg.input should be ("test")
  }
  "Seq2Parquet" should "work on the test seq files" in {
    val cfg = Seq2Parquet.parse_command_line("-i test/small -p sample.seq -o tmp/test_parquet -n 4 --coalesce".split(" ")).get
    print(s"called with arguments\n${cfg.valueTreeString}")


    Seq2Parquet.run(cfg, spark)

    spark.read.parquet("tmp/test_parquet").take(5).foreach(println)
    //Thread.sleep(1000 * 10000)
  }
}
