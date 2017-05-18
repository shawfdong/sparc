package org.jgi.spark.localcluster

/**
  * Created by Lizhen Shi on 5/13/17.
  */

import org.scalatest._

class KmerSpec extends FlatSpec with Matchers {

  "Kmer" should "be equal when the bases are same" in {
    DNASeq.from_bases("ATC") shouldEqual DNASeq.from_bases("ATC")
    DNASeq.from_bases("ATG") should not equal DNASeq.from_bases("ATC")
    DNASeq.from_bases("ATC").hashCode shouldEqual DNASeq.from_bases("ATC").hashCode
    DNASeq.from_bases("ATG").hashCode should not equal DNASeq.from_bases("ATC").hashCode

  }
  it should "think longer kmer is bigger" in {
    DNASeq.from_bases("ATCT") should be < DNASeq.from_bases("ATCTA")
  }

  "Kmer generator" should "generate n-k+1 kmers" in {
    val seq = "ATCGATC"
    val k = 3
    val kmers = Kmer.generate_kmer(seq, k, with_reverse_complements = false, distinct = false)
    kmers.length shouldEqual seq.length - k + 1
  }
  it should "generate 2*(n-k+1) kmers when considering reverse complements" in {
    val seq = "ATCGATC"
    val k = 3
    val kmers = Kmer.generate_kmer(seq, k, with_reverse_complements = true, distinct = false)
    kmers.length shouldEqual (seq.length - k + 1) * 2
  }

  it should "generate less kmers when only counting distinct kmers" in {
    val seq = "ATCGATC"
    val k = 3
    val kmers = Kmer.generate_kmer(seq, k, with_reverse_complements = false, distinct = true)
    println(kmers.length)
    kmers.sorted.foreach(println)
    kmers.length should be < seq.length - k + 1
  }
  it should "also generate less kmers when only counting distinct kmers when counting distinctly" in {
    val seq = "ATCGATC"
    val k = 3
    val kmers = Kmer.generate_kmer(seq, k, with_reverse_complements = true, distinct = true)
    println(kmers.length)
    kmers.sorted.foreach(println)
    kmers.length should be < (seq.length - k + 1) * 2
  }


}


