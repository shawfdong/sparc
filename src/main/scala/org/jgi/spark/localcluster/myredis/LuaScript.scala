package org.jgi.spark.localcluster.myredis

import java.io.InputStream

import com.typesafe.scalalogging.LazyLogging
import redis.clients.jedis.Jedis

/**
  * Created by Lizhen Shi on 5/23/17.
  */
object LuaScript extends LazyLogging {

  def get_script(resource_name: String): String = {
    val stream: InputStream = getClass.getResourceAsStream(resource_name)
    scala.io.Source.fromInputStream(stream).getLines().mkString("\n")

  }

  @transient val sha_dict = collection.mutable.HashMap.empty[String, collection.mutable.HashMap[String, String]]

  val CAS_HINCR = "cas_hincr"
  val scripts = Map(CAS_HINCR -> get_script("/scripts/lua/" + "cas_hincr.lua"))

  def get_sha(script_name: String, jedis: Jedis, jedis_id: String): String = {
    if (!sha_dict.contains(jedis_id)) sha_dict.put(jedis_id, collection.mutable.HashMap.empty[String, String])
    val dict = sha_dict.getOrElse(jedis_id, null)
    if (!dict.contains(script_name)) {
      val sha = jedis.scriptLoad(scripts.getOrElse(script_name, null))
      logger.info(s"load script $script_name for Redis $jedis_id, sha: $sha")
      dict.put(script_name, sha)
    }
    dict.getOrElse(script_name, null)
  }


}
