package org.jgi.spark.localcluster

/**
  * Created by Lizhen Shi on 5/25/17.
  */
case class SingleEdge(src: Int, dest: Int) extends Serializable {

  def this(tuple: (Int, Int)) = this(tuple._1, tuple._2)

  def this(src: Long, dest: Long) = this(src.toInt, dest.toInt)

  override def toString: String = s"$src-$dest"

  override def hashCode: Int = src + dest

}

object SingleEdge extends Serializable {
  def apply(s: String): SingleEdge = {
    val a = s.split("_")
    SingleEdge(a(0).toInt, a(1).toInt)
  }

}

