package org.jgi.spark.localcluster.sandbox

import org.apache.spark.{SparkConf, SparkContext}
import org.jgi.spark.localcluster.{JedisUnitSuite, SparkRedisUnitSuite}
import org.junit.{AfterClass, BeforeClass, Test}
import org.scalatest.{BeforeAndAfterAll, Matchers}
import org.scalatest.junit.JUnitSuite
import com.redislabs.provider.redis._


class TestSparkRedis extends SparkRedisUnitSuite with Matchers {


  @Test
  def test(): Unit = {
    val listRDD = sc.parallelize((0 until 100).map(_.toString))
    sc.toRedisLIST(listRDD, "fruit")
    sc.fromRedisList("fruit").count should be(100)

    val kvRDD = sc.parallelize((0 until 100).map(x => (x.toString, x.toString)))
    sc.toRedisKV(kvRDD)
    sc.fromRedisKV("*").count should be(100)
  }

}



