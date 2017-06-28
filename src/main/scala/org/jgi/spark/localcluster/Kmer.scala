package org.jgi.spark.localcluster

/**
  * Created by Lizhen Shi on 5/13/17.
  */
object Kmer {

  private def canonical_kmer(seq: String) = {
    val rc = DNASeq.reverse_complement(seq)
    if (rc < seq) rc else seq
  }

  // generates kmers hash values given the seq (doesn't handle 'N' and reverse complement)
  private def _generate_kmer(seq: String, k: Int) = {
    (0 until (seq.length - k + 1)).map {
      i =>
        val subseq = seq.substring(i, i + k)
        DNASeq.from_bases(canonical_kmer(subseq))
    }
  }


  // generates kmers
  def generate_kmer(seq: String, k: Int): Array[DNASeq] = {
    seq.split("N").flatMap {
      subSeq => {
        _generate_kmer(subSeq, k)
      }
    }.distinct
  }

  def main(args: Array[String]) = {
    implicit class Crossable[X](xs: Traversable[X]) {
      def cross[Y](ys: Traversable[Y]) = for {x <- xs; y <- ys} yield (x, y)
    }

    def f(a1: Seq[String]): String = {
      val a = a1.map(u => "\"" + u + "\"")
      println(a.size)
      s"Array(${a.mkString(",")})"
    }

    val a = Seq("A", "T", "C", "G")
    println(f(a))

    val b = a.cross(a).map(u => u.productIterator.toSeq.map(_.asInstanceOf[String]).mkString).toSeq.distinct
    println(f(b))

    val c = a.cross(b).map(u => u.productIterator.toSeq.map(_.asInstanceOf[String]).mkString).toSeq.distinct
    println(f(c))

    val d = a.cross(c).map(u => u.productIterator.toSeq.map(_.asInstanceOf[String]).mkString).toSeq.distinct
    println(f(d))
  }
}
