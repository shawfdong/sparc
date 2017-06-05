package org.jgi.spark.localcluster.tools

import org.jgi.spark.localcluster.kvstore.LEVELDBStoreClusterUnitSuite
import org.junit.Test
import org.scalatest.Matchers
import sext._

/**
  * Created by Lizhen Shi on 5/22/17.
  */
class LEVELDBStoreKmerCountingSpec extends LEVELDBStoreClusterUnitSuite with Matchers {
  val KVSTORE_NODES = LEVELDBStoreClusterUnitSuite.ports.map(x => s"127.0.0.1:$x").mkString(",")

  @Test
  def parseCommand(): Unit = {
    val cfg = KmerCounting.parse_command_line("-i test -p *.seq -o tmp --kvstore 127.0.0.1:1234".split(" ")).get
    cfg.input should be("test")

    assertThrows[Exception] {
      KmerCounting.parse_command_line("-i test -p *.seq -o tmp --kvstore 127.0.0.1:12a".split(" ")).get
    }
    assertThrows[Exception] {
      KmerCounting.parse_command_line("-i test -p *.seq -o tmp --kvstore 1.127.0.0.1:1234".split(" ")).get
    }

  }

  @Test
  def kmer_counting_should_work_on_the_test_seq_files() {

    val cfg = KmerCounting.parse_command_line(s"-i test/small -p sample.seq -o tmp/leveldbstore_kmercounting_seq_test.txt --n_iteration 1 --kvstore $KVSTORE_NODES".split(" ")).get
    print(s"called with arguments\n${cfg.valueTreeString}")
    KmerCounting.run(cfg, sc)
  }

  @Test
  def kmer_counting_should_work_on_the_test_seq_files_with_bloomfilter() {

    val cfg = KmerCounting.parse_command_line(s"-i test/small -p sample.seq -o tmp/leveldbstore_kmercounting_seq_test_bf.txt --n_iteration 1 --kvstore $KVSTORE_NODES --use_bloom_filter".split(" ")).get
    print(s"called with arguments\n${cfg.valueTreeString}")
    KmerCounting.run(cfg, sc)
  }

  @Test
  def kmer_counting_should_work_on_the_test_seq_files_with_multiple_N_in() {
    val cfg = KmerCounting.parse_command_line(s"-i test/small -p sample2.seq -o tmp/leveldbstore_kmercounting_seq_sample2.txt --n_iteration 1 -k 15 --kvstore $KVSTORE_NODES".split(" ")).get
    print(s"called with arguments\n${cfg.valueTreeString}")
    KmerCounting.run(cfg, sc)
  }

  @Test
  def kmer_counting_should_work_on_the_test_base64_files_in() {
    val cfg = KmerCounting.parse_command_line(s"-i test/small -p sample.base64 --format base64 -o tmp/leveldbstore_kmercounting_base64_test.txt --n_iteration 3 --kvstore $KVSTORE_NODES".split(" ")).get
    print(s"called with arguments\n${cfg.valueTreeString}")
    KmerCounting.run(cfg, sc)
  }

  @Test
  def kmer_counting_should_work_on_the_test_base64_files_with_multiple_N_in() {
    val cfg = KmerCounting.parse_command_line(s"-i test/small -p sample2.base64 --format base64 -o tmp/leveldbstore_kmercounting_base64_sample2.txt --n_iteration 2 -k 15 --kvstore $KVSTORE_NODES".split(" ")).get
    print(s"called with arguments\n${cfg.valueTreeString}")
    KmerCounting.run(cfg, sc)
  }

}
