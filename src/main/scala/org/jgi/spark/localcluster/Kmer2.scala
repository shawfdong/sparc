package org.jgi.spark.localcluster

/**
  * Created by Lizhen Shi on 5/13/17.
  */
object Kmer2 {

  // generates kmers hash values given the seq (doesn't handle 'N' and reverse complement)
  private def _generate_kmer(seq: String, k: Int) = {
    (0 until (seq.length - k + 1)).map {
      i => DNASeq.from_bases(seq.substring(i, i + k))
    }
  }

  // generates kmers
  def generate_kmer(seq: String, k: Int): Array[DNASeq] = {
    seq.split("N").flatMap {
      subSeq => {
        _generate_kmer(subSeq, k) ++  _generate_kmer(DNASeq.reverse_complement(subSeq), k)
      }
    }.distinct
  }
}
