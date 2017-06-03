package org.jgi.spark.localcluster.kvstore

import org.jgi.spark.localcluster.{DNASeq, SingleEdge}

import scala.language.implicitConversions

/**
  * Created by Lizhen Shi on 6/4/17.
  */
class KVStoreFunction(@transient val mgr: KVStoreManager) {

  // ============================ Kmer counting ===================================
  def incr_batch(keys: collection.Iterable[DNASeq], useBloomFilter: Boolean): Unit = {
    keys.map { x => (x.hashCode % mgr.kvstoreSlots.length, x) }
      .groupBy(_._1).foreach {
      case (hashVal, grouped) =>
        val slot = mgr.getSlot(hashVal)
        val kvstore = mgr.getKVStoreClient(slot)
        try {
          kvstore.incr_kmer_in_batch(grouped.map(_._2).toArray, useBloomFilter = useBloomFilter)

        } finally {
          kvstore.close()
        }
    }
  }

  // ============================ edge counting ===================================
  def incr_edge_batch(keys: collection.Iterable[SingleEdge], useBloomFilter: Boolean): Unit = {
    keys.map { x => (x.hashCode % mgr.kvstoreSlots.length, x) }
      .groupBy(_._1).foreach {
      case (hashVal, grouped) =>
        val slot = mgr.getSlot(hashVal)
        val kvstore = mgr.getKVStoreClient(slot)
        try {
          kvstore.incr_edge_in_batch(grouped.map(_._2).map(x => (x.src, x.dest)).toArray, useBloomFilter = useBloomFilter)
        } finally {
          kvstore.close()
        }
    }
  }
}

trait KVStoreFunTrait {
  implicit def toKVStoreFunction(mgr: KVStoreManager): KVStoreFunction = new KVStoreFunction(mgr)
}
