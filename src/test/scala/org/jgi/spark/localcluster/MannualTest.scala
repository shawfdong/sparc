package org.jgi.spark.localcluster

import java.lang.{Long => JLong}

import org.junit.Test
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by Lizhen Shi on 6/9/17.
  */
class MannualTest extends FlatSpec with Matchers {

  def gen_kmer(s: String, k: Int) = {
    def _f(s: String) = {
      val a = (0 until (s.length - k + 1)).map(i => s.substring(i, i + k))
      val rs = s.reverse.map(DNASeq.rc(_))
      val b = (0 until (rs.length - k + 1)).map(i => rs.substring(i, i + k))
      (a ++ b)
    }

    s.split("N").map(_f).flatten.distinct
  }


  // define a map for reverse complement a sequence
  val rc = Map('A' -> 'T', 'T' -> 'A', 'C' -> 'G', 'G' -> 'C', 'N' -> 'N')
  val base_encoding = Map('A' -> "00", 'T' -> "01", 'C' -> "10", 'G' -> "11")

  // generates kmers hash values given the seq (doesn't handle 'N' and reverse complement)
  def emitKmers(seq: String, k: Int) = {
    (0 until (seq.length - k + 1)).map {
      i => JLong.parseLong(seq.substring(i, i + k).toList.map(base_encoding).mkString(""), 2)
    }
  }

  // generates distinct kmers including reverse complements
  def generate_kmer2(k: Int, seqID: Long, seq: String) = {
    val a = seq.split("N").flatMap { subSeq =>
      (emitKmers(subSeq, k) ++ emitKmers(subSeq.reverse.map(rc(_)), k))
    }.distinct.map {
      x => (x -> List(seqID))
    }
    a
  }

  val seqs = Array("AGGGCAACAGAAGATGATAACAGAAAGGCATTTTCCATGCCTATATTCGGTCGTTTTACCCAGAGAGCCCAGCAGACACTGATGCTGGCCCAGCGGATCGCNTCTGTCCCAGCTTATGGGATTCCAGCACGCTGTTTTCCAGCGTTTTCTTTGCCCTGGGGCTCAGCTCGATCCGGTTTCCTCTGGCCTCTTCCGGATCACCG"
    , "GGGCGGAAACCCGGGCATACGGCGGGGTCCTTCTTCGTTTCCCGCGTTTTCACGCATCTGCCGCAGCAGCTGCTCCCCTGCGGCCGGACGGTGCACAACGGNTCCAGGATCCGGATACGCCCAGGAGCCCCCGGATCGAACTGAGCCCGCGCGCAAAGAAAACGCTGGAAAACAGCGTGCTGGAAAGCCATAAACTGGGACAG")

  @Test
  def test1(): Unit = {
    val seq_kmers = seqs.map(gen_kmer(_, 31))
    val kmers = seq_kmers.flatten.map(x => (x, 1)).groupBy(x => x._1).map(x => (x._1, x._2.map(_._2).sum))
    println("#kmer>1")
    kmers.toArray.filter(_._2 > 1).sortBy(_._1).foreach(println)
    kmers.keys.toSet.size should be(kmers.size)

    println("Common kmers")
    seq_kmers(0).toSet.intersect(seq_kmers(1).toSet).foreach(println)

    println(seq_kmers(0).map(x => "\"" + x + "\"").mkString(","))

  }

  @Test
  def test2(): Unit = {
    var seq_kmers = seqs.map(generate_kmer2(31, 0, _))
    val kmers = seq_kmers.map(x => x.map(_._1)).flatten.map(x => (x, 1)).groupBy(x => x._1).map(x => (x._1, x._2.map(_._2).sum))
    println("#kmer>1")
    kmers.toArray.filter(_._2 > 1).sortBy(_._1).foreach(println)
    kmers.keys.toSet.size should be(kmers.size)

    println("Common kmers")
    seq_kmers(0).toSet.intersect(seq_kmers(1).toSet).foreach(println)
  }

  @Test
  def test3(): Unit = {
    var seq_kmers = seqs.map(Kmer2.generate_kmer(_, 31))
    val kmers = seq_kmers.flatten.map(x => (x, 1)).groupBy(x => x._1).map(x => (x._1, x._2.map(_._2).sum))

    println("#kmer>1")
    kmers.toArray.filter(_._2 > 1).sortBy(_._1).map(_._1).map(_.to_bases(31)).foreach(println)
    kmers.keys.toSet.size should be(kmers.size)

    println("Common kmers")
    seq_kmers(0).toSet.intersect(seq_kmers(1).toSet).map(_.to_bases(31)).foreach(println)
  }

  @Test
  def test3_2(): Unit = {
    val seq = seqs(0)
    val kmers1 = gen_kmer(seq, 31).toSet
    val kmers2 = Kmer2.generate_kmer(seq, 31).map(_.to_bases(31)).toSet
    kmers1.size should be(kmers2.size)
    kmers1 should equal(kmers2)

    DNASeq.reverse_complement("TAAAGAAAACGCTGGAAAACAGCGTGCTGGAA") should equal("TTCCAGCACGCTGTTTTCCAGCGTTTTCTTTA")
  }


  @Test
  def test4(): Unit = {
    var seq_kmers = seqs.map(Kmer.generate_kmer(_, 31))
    val kmers = seq_kmers.flatten.map(x => (x, 1)).groupBy(x => x._1).map(x => (x._1, x._2.map(_._2).sum))

    println("#kmer>1")
    kmers.toArray.filter(_._2 > 1).sortBy(_._1).map(_._1).map(_.to_bases(31)).foreach(println)
    kmers.keys.toSet.size should be(kmers.size)

    println("Common kmers")
    seq_kmers(0).toSet.intersect(seq_kmers(1).toSet).map(_.to_bases(31)).foreach(println)
  }
}
