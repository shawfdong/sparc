package org.jgi.spark.localcluster

/**
  * Created by Lizhen Shi on 5/23/17.
  */
case class RedisSlot(ip: String, port: Int, slot: Int) {
  val ipAndPort: (String, Int) = {
    (ip, port)
  }

  val instance_id = {
    s"${ip}:${port}"
  }

  val id = {
    s"${ip}:${port}:${slot}"
  }

  def key(k: String) = s"${id}-${k}"


  override def toString: String = {
    s"RedisSlot: ip=$ip port=$port slot=$slot"
  }
}
