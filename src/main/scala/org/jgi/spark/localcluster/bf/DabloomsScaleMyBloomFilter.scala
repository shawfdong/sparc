package org.jgi.spark.localcluster.bf

/**
  * Created by Lizhen Shi on 5/31/17.
  */
class DabloomsScaleBloomFilter(expectedElements: Long, falsePositiveRate: Double) {//extends MyBloomFilter(expectedElements, falsePositiveRate) {
  val filter = new com.github.jdablooms.ScaleBloomFilter(expectedElements, falsePositiveRate)

    def put(o: Array[Byte]): Unit = {
    filter.put(o)
  }

  def put(o: String): Unit = {
    filter.put(o)
  }

    def mightContain(o: Array[Byte]): Boolean = {
    filter.mightContain(o)
  }

  def mightContain(o: String): Boolean = {
    filter.mightContain(o)
  }

  def size(): Long = filter.count()

    def close: Unit = filter.close()
}
