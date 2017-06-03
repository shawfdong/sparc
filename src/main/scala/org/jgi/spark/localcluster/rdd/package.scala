package org.jgi.spark.localcluster

import org.jgi.spark.localcluster.rdd.RedisFunctions

/**
  * Created by Lizhen Shi on 5/23/17.
  */
package object rdd  extends RedisFunctions with KVStoreFunctions
