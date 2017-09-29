package org.jgi.spark.localcluster.tools

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.SparkConf
import org.scalatest.{FlatSpec, Matchers}
import sext._

/**
  * Created by Lizhen Shi on 5/14/17.
  */
class KmerCounting2Spec extends FlatSpec with Matchers  with SharedSparkContext
{

  "parse command line" should "be good" in {
    val cfg = KmerCounting2.parse_command_line("-k 31 -i test -p *.seq -o tmp".split(" ")).get
    cfg.input should be("test")
  }
  "kmer counting" should "work on the test seq files" in {
    val cfg = KmerCounting2.parse_command_line("-i test/small -k 31 -p sample.seq -o tmp/kmercounting2_seq_test.txt --n_iteration 1".split(" ")).get
    print(s"called with arguments\n${cfg.valueTreeString}")
    KmerCounting2.run(cfg, sc)
  }

  "kmer counting" should "work on the test seq files with canonical_kmer" in {
    val cfg = KmerCounting2.parse_command_line("-i test/small -k 31 -p sample.seq -o tmp/kmercounting2_seq_test_C.txt --n_iteration 1".split(" ")).get
    print(s"called with arguments\n${cfg.valueTreeString}")
    KmerCounting2.run(cfg, sc)
  }

  "kmer counting" should "work on the test seq files with multiple N" in {
    val cfg = KmerCounting2.parse_command_line("-i test/small -k 31 -p sample2.seq -o tmp/kmercounting2_seq_sample2.txt --n_iteration 1".split(" ")).get
    print(s"called with arguments\n${cfg.valueTreeString}")
    KmerCounting2.run(cfg, sc)
  }


  "kmer counting" should "work on the test base64 files" in {
    val cfg = KmerCounting2.parse_command_line("-i test/small -k 31 -p sample.base64 --format base64 -o tmp/kmercounting2_base64_test.txt --n_iteration 4".split(" ")).get
    print(s"called with arguments\n${cfg.valueTreeString}")
    KmerCounting2.run(cfg, sc)
  }

  "kmer counting" should "work on the test base64 files with canonical_kmer" in {
    val cfg = KmerCounting2.parse_command_line("-i test/small -k 31 -p sample.base64 --format base64 -o tmp/kmercounting2_base64_test_C.txt --n_iteration 4".split(" ")).get
    print(s"called with arguments\n${cfg.valueTreeString}")
    KmerCounting2.run(cfg, sc)
  }

  "kmer counting" should "work on the test base64 files with multiple N" in {
    val cfg = KmerCounting2.parse_command_line("-i test/small -k 31 -p sample2.base64 --format base64 -o tmp/kmercounting2_base64_sample2.txt --n_iteration 4".split(" ")).get
    print(s"called with arguments\n${cfg.valueTreeString}")
    KmerCounting2.run(cfg, sc)
  }

  //*************************************** fake
  "kmer counting" should "work on fake seq file" in {
    val cfg = KmerCounting2.parse_command_line("-i test/small -k 31 -p fake_sample.seq -o tmp/kmercounting2_seq_fake.txt --n_iteration 1".split(" ")).get
    print(s"called with arguments\n${cfg.valueTreeString}")
    KmerCounting2.run(cfg, sc)
  }

}
