package org.jgi.spark.localcluster.tools

import com.holdenkarau.spark.testing.SharedSparkContext
import org.jgi.spark.localcluster.RedisClusterUnitSuite
import org.junit.Test
import org.scalatest.{Matchers, _}
import sext._

/**
  * Created by Lizhen Shi on 5/22/17.
  */
class RedisKmerCountingSpec extends RedisClusterUnitSuite with Matchers {
  private val master = "local[4]"
  private val appName = "RedisKmerCountingSpec"
  val REDIS_NODE = "127.0.0.1:42000"

  @Test
  def parseCommand(): Unit = {
    val cfg = KmerCounting.parse_command_line("-i test -p *.seq -o tmp --redis 127.0.0.1:1234".split(" ")).get
    cfg.input should be("test")
    cfg.redis_port shouldEqual 1234

    assertThrows[Exception] {
      KmerCounting.parse_command_line("-i test -p *.seq -o tmp --redis 127.0.0.1:12a".split(" ")).get
    }
    assertThrows[Exception] {
      KmerCounting.parse_command_line("-i test -p *.seq -o tmp --redis 1.127.0.0.1:1234".split(" ")).get
    }

  }

  @Test
  def kmer_counting_should_work_on_the_test_seq_files {

    val cfg = KmerCounting.parse_command_line(s"-i test/small -p sample.seq -o tmp/redis_kmercounting_seq_test.txt --n_iteration 1 --sample_fraction 0.2 --redis $REDIS_NODE".split(" ")).get
    print(s"called with arguments\n${cfg.valueTreeString}")
    sc.getConf.set("redis.host", cfg.redis_ip).set("redis.port", cfg.redis_port.toString)
    KmerCounting.run(cfg, sc)
  }

  @Test
  def kmer_counting_should_work_on_the_test_seq_files_with_multiple_N_in {
    val cfg = KmerCounting.parse_command_line(s"-i test/small -p sample2.seq -o tmp/redis_kmercounting_seq_sample2.txt --n_iteration 1 -k 15 --redis $REDIS_NODE".split(" ")).get
    print(s"called with arguments\n${cfg.valueTreeString}")
    KmerCounting.run(cfg, sc)
  }

  @Test
  def kmer_counting_should_work_on_the_test_base64_files_in {
    val cfg = KmerCounting.parse_command_line(s"-i test/small -p sample.base64 --format base64 -o tmp/redis_kmercounting_base64_test.txt --n_iteration 3 --redis $REDIS_NODE".split(" ")).get
    print(s"called with arguments\n${cfg.valueTreeString}")
    KmerCounting.run(cfg, sc)
  }

  @Test
  def kmer_counting_should_work_on_the_test_base64_files_with_multiple_N_in {
    val cfg = KmerCounting.parse_command_line(s"-i test/small -p sample2.base64 --format base64 -o tmp/redis_kmercounting_base64_sample2.txt --n_iteration 2 -k 15 --redis $REDIS_NODE".split(" ")).get
    print(s"called with arguments\n${cfg.valueTreeString}")
    KmerCounting.run(cfg, sc)
  }

}
