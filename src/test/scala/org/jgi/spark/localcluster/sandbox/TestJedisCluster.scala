package org.jgi.spark.localcluster.sandbox

import java.util

import org.jgi.spark.localcluster._
import org.junit.Test
import org.scalatest.Matchers

/**
  * Created by Lizhen Shi on 5/21/17.
  */
class TestJedisCluster extends RedisClusterUnitSuite with Matchers {


  @Test def testCluster(): Unit = {
    println("start test on testCluster")
    jedisMgr.set("foo", "bar")
    jedisMgr.set("test", "test")
    jedisMgr.get("foo") shouldEqual "bar"
    jedisMgr.get("test") shouldEqual "test"
    jedisMgr.set_batch(List("a","b"),List("1","2"))
    jedisMgr.get("a") shouldEqual "1"
    jedisMgr.get("b") shouldEqual "2"
    jedisMgr.get("non exists") should be (null)
    jedisMgr.incr_batch(List("CCG","ATG","TAG","ATG").map(DNASeq.from_bases(_)))
    jedisMgr.get(DNASeq.from_bases("ATG")) shouldEqual "2"
    jedisMgr.get(DNASeq.from_bases("ATGAAAAAAAAA")) shouldEqual  (null)
  }

  @Test
  def test_lua_script(): Unit ={
    val jedis = jedisMgr.getJedis(jedisMgr.getSlot(0))
    val name ="/scripts/lua/" + "cas_hincr.lua"
    val script=LuaScript.get_script(name)
    println(script)
    script should not be (null)
    val sha = LuaScript.get_sha("cas_hincr",jedis, 0.toString)
  }


}
