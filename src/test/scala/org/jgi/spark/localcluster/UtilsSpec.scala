package org.jgi.spark.localcluster

/**
  * Created by Lizhen Shi on 5/21/17.
  */

import java.util

import org.scalatest._

import collection.JavaConversions._


class UtilsSpec extends FlatSpec with Matchers {
  "IP address" should "be found" in {
    print(s"IP address of this machine is ${JavaUtils.getLocalHostLANAddress.toString}")
  }

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
}
