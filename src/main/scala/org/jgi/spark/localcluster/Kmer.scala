package org.jgi.spark.localcluster

/**
  * Created by Lizhen Shi on 5/13/17.
  */
object Kmer {

  // generates kmers hash values given the seq (doesn't handle 'N' and reverse complement)
  private def _generate_kmer(seq: String, k: Int) = {
    (0 until (seq.length - k + 1)).map{
      i => DNASeq.from_bases(seq.substring(i, i + k))
    }
  }

  // generates kmers
  def generate_kmer( seq: String,k: Int, with_reverse_complements:Boolean=true, distinct:Boolean=true ): Array[DNASeq] = {
    val results = seq.split("N").flatMap { subSeq => {
      val kmers = _generate_kmer(subSeq, k)
      if (with_reverse_complements) kmers ++ _generate_kmer(subSeq.reverse.map(DNASeq.rc(_)), k) else kmers
    }
    }
    if (distinct) results.distinct else results
  }
}
