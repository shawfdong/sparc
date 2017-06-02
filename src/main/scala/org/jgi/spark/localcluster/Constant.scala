package org.jgi.spark.localcluster

/**
  * Created by Lizhen Shi on 5/24/17.
  */
object Constant {
  var BLOOMFILTER_EXPECTED_POSITIVE_FALSE: String = 0.01.toString

  var BLOOMFILTER_EXPECTED_NUM_ITEMS: String = (1000*1000*100).toString

  val KMER_COUNTING_REDIS_BLOOMFILTER_HASH_KEY: _root_.scala.Predef.String = "bfhkc"
  val KMER_COUNTING_REDIS_BLOOMFILTER_COUNT_HASH_KEY: _root_.scala.Predef.String = "bfhkc:count"
  val KMER_COUNTING_REDIS_HASH_KEY:  String = "hkc"

  val EDGE_COUNTING_REDIS_BLOOMFILTER_HASH_KEY: _root_.scala.Predef.String = "bfhec"
  val EDGE_COUNTING_REDIS_BLOOMFILTER_COUNT_HASH_KEY: _root_.scala.Predef.String = "bfhec:count"
  val EDGE_COUNTING_REDIS_HASH_KEY:  String = "hec"

}
