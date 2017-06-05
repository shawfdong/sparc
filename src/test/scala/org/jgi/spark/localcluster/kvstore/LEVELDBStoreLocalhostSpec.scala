package org.jgi.spark.localcluster.kvstore

import org.junit.Test
import org.scalatest.Matchers
import org.scalatest.junit.JUnitSuite

/**
  * Created by Lizhen Shi on 5/22/17.
  */
class LEVELDBStoreLocalhostSpec extends JUnitSuite with Matchers {


  @Test
  def testSingleServer(): Unit = {
    val port=33231
    val server =new KVStoreLocalhostServer(port,"leveldb")
    new Thread(server).start()
    Thread.sleep(1000)
    server.shutdown()

  }

  @Test
  def testTwoServer(): Unit = {
    val port=33234
    val server =new KVStoreLocalhostServer(port,"leveldb")
    new Thread(server).start()
    val server2 =new KVStoreLocalhostServer(port+2,"leveldb")
    new Thread(server2).start()

    Thread.sleep(1000)
    server.shutdown()
    server2.shutdown()
  }

}
