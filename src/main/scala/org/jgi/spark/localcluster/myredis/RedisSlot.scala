package org.jgi.spark.localcluster.myredis

/**
  * Created by Lizhen Shi on 5/23/17.
  */
case class RedisSlot(ip: String, port: Int, slot: Int) {
  val ipAndPort: (String, Int) = {
    (ip, port)
  }

  val instance_id: String = {
    s"${ip}:${port}"
  }

  val id: String = {
    s"${ip}:${port}:${slot}"
  }

  def key(k: String) = s"${id}-${k}"


  override def toString: String = {
    s"RedisSlot: ip=$ip port=$port slot=$slot"
  }
}
