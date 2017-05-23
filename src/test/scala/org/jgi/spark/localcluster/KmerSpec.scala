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
    val kmers = Kmer.generate_kmer(seq, k)
    kmers.length shouldEqual seq.length - k + 1
  }


}


