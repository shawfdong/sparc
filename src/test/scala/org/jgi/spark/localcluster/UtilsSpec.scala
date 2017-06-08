package org.jgi.spark.localcluster

/**
  * Created by Lizhen Shi on 5/21/17.
  */

import java.nio.ByteBuffer
import java.util

import com.google.common.hash.Hashing
import de.greenrobot.common.hash.Murmur3A
import org.scalatest._

import collection.JavaConversions._
import scala.util.Random


class UtilsSpec extends FlatSpec with Matchers {
  val N=1000*1000*10

  "All ip addresses" should "be found" in {
    JavaUtils.getAllIPs.foreach(println)
  }

  "loopback ip" should "be matched" in {
    val l = new util.ArrayList[String]
    l.append("127.0.0.1")
    JavaUtils.getMatchedIP(l) shouldEqual "127.0.0.1"
  }

  "pattern match" should "also work" in {
    JavaUtils.getMatchedIP("127.0.*") shouldEqual "127.0.0.1"
  }

  "gogole hash test" should "work" in {
    val bytes = new Array[Byte](32)
    val start = System.currentTimeMillis()
    0.until(N).foreach {
      _=>
      Random.nextBytes(bytes)
      Hashing.murmur3_32().hashBytes(bytes).asInt()
    }
    val dt:Double = (System.currentTimeMillis()-start)/1000.0
    println(s"guava takes $dt seconds")
  }

  "java common hash test" should "work" in {
     val bytes = new Array[Byte](32)
    val start = System.currentTimeMillis()
    0.until(N).foreach {
      _=>
        Random.nextBytes(bytes)
        val murmur3a = new Murmur3A()
        murmur3a.update(bytes)
        murmur3a.getValue.toInt
    }
    val dt:Double = (System.currentTimeMillis()-start)/1000.0
    println(s"java common takes $dt seconds")
  }

  "scala hash" should "work" in {

    val bytes = new Array[Byte](32)
    val start = System.currentTimeMillis()
    0.until(N).foreach {
      _=>
        bytes.map(_.hashCode()).sum
    }
    val dt:Double = (System.currentTimeMillis()-start)/1000.0
    println(s"scala hash takes $dt seconds")
  }


  "bytes to int hash" should "work" in {

    val bytes = new Array[Byte](32)
    val start = System.currentTimeMillis()
    0.until(N).foreach {
      _=>
        java.nio.ByteBuffer.wrap(bytes.take(4)).getInt()
    }
    val dt:Double = (System.currentTimeMillis()-start)/1000.0
    println(s"scala hash takes $dt seconds")
  }

  "test bytes hash equal" should "work" in {
    val bytes =ByteBuffer.allocate(4).putInt(1234566).array
    println(bytes.length)
    val bytes2 =ByteBuffer.allocate(4).putInt(1234566).array

    (bytes.equals(bytes2)) should be (false)
    bytes.hashCode() should not be (bytes2.hashCode())


  }
}
