package org.jgi.spark.localcluster

/**
  * Created by Lizhen Shi on 5/13/17.
  */

import org.scalatest._

class DNASeqSpec extends FlatSpec with Matchers {

  "int" should "be converted to base64 string and converted back" in {
    val r = new scala.util.Random
    for (_ <- 0 until 100) {
      val i = r.nextInt()
      val s = DNASeq.int_to_base64(i)
      val j = DNASeq.base64_to_int(s)
      j shouldEqual i
    }

  }

  "long" should "be converted to base64 string and converted back" in {
    val r = new scala.util.Random
    for (_ <- 0 until 100) {
      val i = r.nextLong()
      val s = DNASeq.long_to_base64(i)
      val j = DNASeq.base64_to_long(s)
      j shouldEqual i
    }

  }

  "DNASeq of 4 bases" should "be able to be converted into a bytes array of length 1" in {

    Integer.parseInt("11111111", 2) shouldEqual 255
    Integer.parseInt("00000000", 2) shouldEqual 0

    def f(bases: String, bins: String) = {
      println(bases, bins)
      val kmer = DNASeq.from_bases(bases)
      kmer.bytes.length shouldEqual 1
      DNASeq.int8_to_uint8(kmer.bytes(0)) shouldEqual Integer.parseInt(bins, 2)
    }

    f("ATCG", "00011011")
    f("GTCG", "11011011")
    f("TTCG", "01011011")
    f("CTCG", "10011011")
    f("GGGG", "11111111")
    f("AAAA", "00000000")
    f("ATC", "00011000")


  }

  "DNASeq of 8 bases" should "be able to be converted into a bytes array of length 2" in {
    def f(bases: String, bins: List[String]) = {
      println(bases, bins.mkString)
      val kmer = DNASeq.from_bases(bases)
      kmer.bytes.length shouldEqual 2
      DNASeq.int8_to_uint8(kmer.bytes(0)) shouldEqual Integer.parseInt(bins.head, 2)
      DNASeq.int8_to_uint8(kmer.bytes(1)) shouldEqual Integer.parseInt(bins(1), 2)

    }

    f("ATCG" + "GTCG", List("00011011", "11011011"))
    f("TTCG" + "AAAA", List("01011011", "00000000"))


  }

  "A DNASeq" should "be able to be converted bytes and convert bytes back exatly" in {
    (0 until 100) foreach (_ => {
      val (bases: String, k: Int) = DNASeqSpec.random_kmer_bases()
      val kmer = DNASeq.from_bases(bases)
      kmer.to_bases(k) shouldEqual bases
    })
  }

  it should "be  able to be converted to base64 strings and convert back correctly" in {
    (0 until 100) foreach (i => {
      val (bases: String, k: Int) = DNASeqSpec.random_kmer_bases()
      val kmer = DNASeq.from_bases(bases)
      kmer.to_bases(k) shouldEqual bases
      val base64String = kmer.to_base64
      if (i < 5) println(kmer.bytes.length * 4, base64String.length, base64String)
      val kmer2 = DNASeq.from_base64(base64String)
      kmer2 shouldEqual kmer
    })
  }
}

object DNASeqSpec {
  def random_kmer_bases(): (String, Int) = {
    val r = new scala.util.Random
    random_kmer_bases(math.abs(r.nextInt()) % 70 + 1)
  }

  def random_kmer_bases(k: Int): (String, Int) = {
    val r = new scala.util.Random
    val kmer = (0 until k).map(_ => math.abs(r.nextInt()) % 4).map(DNASeq.inverse_base_encoding(_)).mkString
    (kmer, k)
  }
}



