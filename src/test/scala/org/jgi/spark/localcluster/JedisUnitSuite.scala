package org.jgi.spark.localcluster

import com.lordofthejars.nosqlunit.redis.ManagedRedis.ManagedRedisRuleBuilder.newManagedRedisRule
import com.lordofthejars.nosqlunit.redis.RedisRule.RedisRuleBuilder.newRedisRule
import org.junit.{ClassRule, Rule}
import org.scalatest.junit.JUnitSuite
import redis.clients.jedis.JedisPool
import redis.clients.jedis.JedisPoolConfig

/**
  * Created by Lizhen Shi on 5/18/17.
  */
abstract class JedisUnitSuite extends JUnitSuite {
  val redisRule_ = newRedisRule.defaultManagedRedis

  @Rule
  def redisRule = redisRule_

  def pool = JedisUnitSuite.pool

}

object JedisUnitSuite extends JUnitSuite {
  val redis_home = System.getenv("REDIS_HOME")
  if (redis_home == null) throw new RuntimeException("REDIS_HOME is not set")
  System.setProperty("REDIS_HOME", redis_home)

  @ClassRule
  def managedRedis = newManagedRedisRule.build


  val pool = new JedisPool(new JedisPoolConfig, "localhost")

  Runtime.getRuntime.addShutdownHook(new Thread(new Runnable() {
    def run() {
      println("jvm exiting, destroying Jedis pool")
      pool.destroy()
    }
  }))
}