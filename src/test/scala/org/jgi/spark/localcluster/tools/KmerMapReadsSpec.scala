package org.jgi.spark.localcluster.tools

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers, _}
import sext._

/**
  * Created by Lizhen Shi on 5/17/17.
  */
class KmerMapReadsSpec extends FlatSpec with Matchers with BeforeAndAfter with SharedSparkContext
{
  private val master = "local[4]"
  private val appName = "KmerMapReadsSpec"


  "parse command line" should "be good" in {
    val cfg = KmerMapReads.parse_command_line("--reads test -p *.seq --kmer test/kmercounting_test.txt -k 31 -o tmp".split(" ")).get
    cfg.reads_input should be("test")
  }
  "kmer mapping" should "work on the test seq files" in {
    val cfg = KmerMapReads.parse_command_line(
      "--reads test/small -p *.seq --kmer test/kmercounting_test.txt -k 31  -o tmp/kmermapping_seq_test.txt --n_iteration 1".split(" ")
        .filter(_.nonEmpty)).get
    println(s"called with arguments\n${cfg.valueTreeString}")

    KmerMapReads.run(cfg, sc)
    //Thread.sleep(1000 * 10000)
  }

  "kmer mapping" should "work on the test base64 files" in {
    val cfg = KmerMapReads.parse_command_line(
      "--reads test/small -p *.base64 --format base64 --kmer test/kmercounting_test.txt -k 31  -o tmp/kmermapping_base64_test.txt --n_iteration 1".split(" ")
        .filter(_.nonEmpty)).get
    println(s"called with arguments\n${cfg.valueTreeString}")

    KmerMapReads.run(cfg, sc)
    //Thread.sleep(1000 * 10000)
  }
}
