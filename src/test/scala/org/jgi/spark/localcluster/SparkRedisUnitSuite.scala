package org.jgi.spark.localcluster

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{AfterClass, BeforeClass, Rule}

/**
  * Created by Lizhen Shi on 5/18/17.
  */
abstract class SparkRedisUnitSuite extends JedisUnitSuite {
  def sc=SparkRedisUnitSuite.sc

}

object SparkRedisUnitSuite extends JedisUnitSuite {
  var sc: SparkContext = _

  @BeforeClass
  def beforeAll() {
    println("beforeall")
    sc = new SparkContext(new SparkConf()
      .setMaster("local").setAppName(getClass.getName)
      .set("redis.host", "127.0.0.1")
      .set("redis.port", "6379")
    )
  }

  @AfterClass
  def afterAll(): Unit = {
    sc.stop
    System.clearProperty("spark.driver.port")
    println("afterAll")

  }
}

