package org.jgi.spark.localcluster.tools

import org.jgi.spark.localcluster.myredis.RedisClusterUnitSuite
import org.junit.Test
import org.scalatest.Matchers
import sext._

/**
  * Created by Lizhen Shi on 5/25/17.
  */
class RedisGraphGenSpec extends RedisClusterUnitSuite with Matchers {
  private val master = "local[4]"
  private val appName = "GraphGenSpec"
  val REDIS_NODES = "127.0.0.1:42000,127.0.0.1:42001,127.0.0.1:42002"


  @Test
  def parse_command_line_should_be_good_in {
    val cfg = GraphGen.parse_command_line("-i test/kmermapping_test.txt -k 31 -o tmp".split(" ")).get
    cfg.kmer_reads should be("test/kmermapping_test.txt")
  }

  @Test
  def graph_gen_should_work_on_the_test_seq_files_in {
    val cfg = GraphGen.parse_command_line(
      s"-i test/kmermapping_test.txt -k 31 -o tmp/redis_graph_gen_test.txt --n_iteration 1 --redis $REDIS_NODES".split(" ")
        .filter(_.nonEmpty)).get
    println(s"called with arguments\n${cfg.valueTreeString}")

    GraphGen.run(cfg, sc)
  }

  @Test
  def graph_gen_should_work_on_the_test_seq_files_for_multiple_iterations_in {
    val cfg = GraphGen.parse_command_line(
      s"-i test/kmermapping_test.txt -k 31 -o tmp/redis_graph_gen_test_iter3.txt --n_iteration 3 --redis $REDIS_NODES".split(" ")
        .filter(_.nonEmpty)).get
    println(s"called with arguments\n${cfg.valueTreeString}")

    GraphGen.run(cfg, sc)
  }

  @Test
  def graph_gen_should_work_on_the_test_seq_files_for_multiple_iterations_and_use_bloomfilter_in {
    val cfg = GraphGen.parse_command_line(
      s"-i test/kmermapping_test.txt -k 31 -o tmp/redis_graph_gen_test_bf_iter3.txt --n_iteration 3 --use_bloom_filter --redis $REDIS_NODES".split(" ")
        .filter(_.nonEmpty)).get
    println(s"called with arguments\n${cfg.valueTreeString}")

    GraphGen.run(cfg, sc)
  }
}
