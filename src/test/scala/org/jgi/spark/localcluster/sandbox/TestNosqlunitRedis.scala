package org.jgi.spark.localcluster.sandbox

import org.jgi.spark.localcluster.JedisUnitSuite
import org.junit.{ClassRule, Rule, Test}


class TestNosqlunitRedis extends JedisUnitSuite {


  @Test
  def test(): Unit = {

    val jedis = pool.getResource
    try { /// ... do stuff here ... for example
      jedis.set("foo", "bar")
      val foobar = jedis.get("foo")
      jedis.zadd("sose", 0, "car")
      jedis.zadd("sose", 0, "bike")
      val sose = jedis.zrange("sose", 0, -1)
      jedis.set("foo", "bar")
      val v = jedis.get("foo")
      println(v)

    } finally
      if (jedis != null) jedis.close()


  }


}

