package org.jgi.spark.localcluster

import java.util.UUID

import bloomfilter.mutable.{BloomFilter => ScalaBloomFilter, CuckooFilter => ScalaCuckooFilter}

/**
  * Created by Lizhen Shi on 5/25/17.
  */

@SerialVersionUID(789L)
abstract class BloomFilter[T](val expectedElements: Int, val falsePositiveRate: Double) extends Serializable {

  val id = UUID.randomUUID().toString

  var length = 0

  def mightContain(o: T): Boolean

  final def contains(o: T): Boolean = mightContain(o)

  def  _add(x: T): Unit

  final def  add(x: T): Unit = {
    _add(x)
    length+=1
  }

  override def finalize(): Unit = {
    println(s"Disposing ${this.getClass.getName} $id")
  }

  println(s"Create ${this.getClass.getName} $id with [$expectedElements, $falsePositiveRate]")
}


@SerialVersionUID(789L)
class BloomFilterBytes(expectedElements: Int, falsePositiveRate: Double)
  extends BloomFilter[Array[Byte]](expectedElements, falsePositiveRate) {
  val underlying = ScalaBloomFilter[Array[Byte]](expectedElements, falsePositiveRate)

  def mightContain(o: Array[Byte]): Boolean = underlying.mightContain(o)

  def _add(x: Array[Byte]): Unit = underlying.add(x)

  override def finalize(): Unit = {
    underlying.dispose()
    super.finalize()
  }
}



@SerialVersionUID(789L)
class CuckooFilterBytes(expectedElements: Int, falsePositiveRate: Double)
  extends BloomFilter[Array[Byte]](expectedElements, falsePositiveRate) {
  val underlying = ScalaCuckooFilter[Array[Byte]](expectedElements)

  def mightContain(o: Array[Byte]) = underlying.mightContain(o)

  def _add(x: Array[Byte]): Unit = underlying.add(x)

  override def finalize(): Unit = {
    underlying.dispose()
    super.finalize()
  }
}



@SerialVersionUID(789L)
class BloomFilterString(expectedElements: Int, falsePositiveRate: Double)
  extends BloomFilter[String](expectedElements, falsePositiveRate) {
  val underlying = ScalaBloomFilter[String](expectedElements, falsePositiveRate)

  def mightContain(o: String): Boolean = underlying.mightContain(o)

  def _add(x: String): Unit = underlying.add(x)

  override def finalize(): Unit = {
    underlying.dispose()
    super.finalize()
  }
}

@SerialVersionUID(789L)
class CuckooFilterString(expectedElements: Int, falsePositiveRate: Double)
  extends BloomFilter[String](expectedElements, falsePositiveRate) {

  val underlying = ScalaCuckooFilter[String](expectedElements)

  def mightContain(o: String) = underlying.mightContain(o)

  def _add(x: String): Unit = underlying.add(x)

  override def finalize(): Unit = {
    underlying.dispose()
    super.finalize()
  }
}
