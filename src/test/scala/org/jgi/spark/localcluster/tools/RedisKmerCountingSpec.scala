package org.jgi.spark.localcluster.tools

import org.jgi.spark.localcluster.myredis.RedisClusterUnitSuite
import org.junit.Test
import org.scalatest.Matchers
import sext._

/**
  * Created by Lizhen Shi on 5/22/17.
  */
class RedisKmerCountingSpec extends RedisClusterUnitSuite with Matchers {
  val REDIS_NODES = "127.0.0.1:42000,127.0.0.1:42001,127.0.0.1:42002"
  private val master = "local[4]"
  private val appName = "RedisKmerCountingSpec"

  @Test
  def parseCommand(): Unit = {
    val cfg = KmerCounting.parse_command_line("-i test -p *.seq -o tmp --redis 127.0.0.1:1234".split(" ")).get
    cfg.input should be("test")

    assertThrows[Exception] {
      KmerCounting.parse_command_line("-i test -p *.seq -o tmp --redis 127.0.0.1:12a".split(" ")).get
    }
    assertThrows[Exception] {
      KmerCounting.parse_command_line("-i test -p *.seq -o tmp --redis 1.127.0.0.1:1234".split(" ")).get
    }

  }

  @Test
  def kmer_counting_should_work_on_the_test_seq_files {

    val cfg = KmerCounting.parse_command_line(s"-i test/small -p sample.seq -o tmp/redis_kmercounting_seq_test.txt --n_iteration 1 --redis $REDIS_NODES".split(" ")).get
    print(s"called with arguments\n${cfg.valueTreeString}")
    KmerCounting.run(cfg, sc)
  }

  @Test
  def kmer_counting_should_work_on_the_test_seq_files_with_multiple_N_in {
    val cfg = KmerCounting.parse_command_line(s"-i test/small -p sample2.seq -o tmp/redis_kmercounting_seq_sample2.txt --n_iteration 1 -k 15 --redis $REDIS_NODES".split(" ")).get
    print(s"called with arguments\n${cfg.valueTreeString}")
    KmerCounting.run(cfg, sc)
  }

  @Test
  def kmer_counting_should_work_on_the_test_base64_files_in {
    val cfg = KmerCounting.parse_command_line(s"-i test/small -p sample.base64 --format base64 -o tmp/redis_kmercounting_base64_test.txt --n_iteration 3 --redis $REDIS_NODES".split(" ")).get
    print(s"called with arguments\n${cfg.valueTreeString}")
    KmerCounting.run(cfg, sc)
  }

  @Test
  def kmer_counting_should_work_on_the_test_base64_files_with_multiple_N_in {
    val cfg = KmerCounting.parse_command_line(s"-i test/small -p sample2.base64 --format base64 -o tmp/redis_kmercounting_base64_sample2.txt --n_iteration 2 -k 15 --redis $REDIS_NODES".split(" ")).get
    print(s"called with arguments\n${cfg.valueTreeString}")
    KmerCounting.run(cfg, sc)
  }

}
