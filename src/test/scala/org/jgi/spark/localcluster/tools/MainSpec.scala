package org.jgi.spark.localcluster.tools

import org.scalatest.{FlatSpec, Matchers, _}

/**
  * Created by Lizhen Shi on 5/29/17.
  */
class MainSpec extends FlatSpec with Matchers with BeforeAndAfter {

  "help" should "be good" in {
      Main.main("-h".split(" "))
  }

}
