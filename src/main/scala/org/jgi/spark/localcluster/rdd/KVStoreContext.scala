package org.jgi.spark.localcluster.rdd

import org.apache.spark.SparkContext
import org.jgi.spark.localcluster.kvstore.KVStoreSlot

import scala.language.implicitConversions

/**
  * Created by Lizhen Shi on 6/3/17.
  */
class KVStoreContext(@transient val sc: SparkContext) extends Serializable {

  def kmerCountFromKVStore(slots:Array[KVStoreSlot],useBloomFilter:Boolean, minimumCount:Int):
  KVStoreKmerCountRDD = {
    new KVStoreKmerCountRDD(sc,slots,useBloomFilter,minimumCount)
  }

  //////////////////// edge
  def edgeCountFromKVStore(slots:Array[KVStoreSlot],useBloomFilter:Boolean, minimumCount:Int):
  KVStoreEdgeCountRDD = {
    new KVStoreEdgeCountRDD(sc,slots,useBloomFilter,minimumCount)
  }

}

trait KVStoreFunctions {
  implicit def toKVStoreContext(sc: SparkContext): KVStoreContext = new KVStoreContext(sc)
}
