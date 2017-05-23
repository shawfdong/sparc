package org.jgi.spark.localcluster.sandbox

import java.util

import org.jgi.spark.localcluster.{DNASeq, JedisManager, RedisClusterUnitSuite, RedisUtil}
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


}
