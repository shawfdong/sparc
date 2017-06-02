package org.jgi.spark.localcluster.kvstore

/**
  * Created by Lizhen Shi on 6/2/17.
  */
case class KVStoreSlot(ip: String, port: Int) {
  val ipAndPort: (String, Int) = {
    (ip, port)
  }

  val instance_id: String = {
    s"${ip}:${port}"
  }

  val id: String = {
    s"${ip}:${port}"
  }

  def key(k: String) = s"${id}-${k}"


  override def toString: String = {
    s"KVstoreSlot: ip=$ip port=$port"
  }
}
