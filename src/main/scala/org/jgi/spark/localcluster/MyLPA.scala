package org.jgi.spark.localcluster

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import scala.util.control.Breaks._
import java.util.UUID.randomUUID

object MyLPA extends LazyLogging {


  def logInfo(str: String) = {
    logger.info(str)
    //println("AAAA " + str)
  }

  var curr_checkpoint_path: String = null

  //return node,cluster pair
  def run(edges: RDD[(Int, Int)], sqlContext: SQLContext, max_iteration: Int, checkpoint_dir: String): RDD[(Int, Int)] = {
    val spark = sqlContext.sparkSession

    require(max_iteration > 0, s"Maximum of steps must be greater than 0, but got ${max_iteration}")

    // add self linked edges
    val selfEdges = edges.map(_._1).distinct().map(u => (u, u))
    val edgeTuples = edges.union(selfEdges)
    val schema = new StructType()
      .add(StructField("src", IntegerType, false))
      .add(StructField("dest", IntegerType, false))
    val rawdf = spark.createDataFrame(edgeTuples.map { u => Row.fromTuple(u) }, schema)
    var df = checkpoint(rawdf.withColumn("cid", column("src"))
      .withColumn("changed", lit(true)), checkpoint_dir)

    logInfo("dataframe schema:")
    df.printSchema()
    df.show(10)
    import spark.implicits._
    breakable {

      for (i <- 1 to max_iteration) {
        df = checkpoint(run_iteration(df), checkpoint_dir)
        //df.where($"changed" === true).show(10)

        val cnt = df.agg(sum(col("changed").cast("long"))).first.get(0)
        logInfo(s"${cnt} edges changed it's cluster at iteration ${i}")
        if (cnt == 0) {
          logInfo(s"Stop at iteration ${i}")
          break
        }
      }
    }

    make_clusters(df).rdd.map(u => (u.getAs("node_id"), u.getAs("new_cid")))
  }

  def delete_hdfs_file(filepath: String): Unit = {
    import org.apache.hadoop.conf.Configuration
    import org.apache.hadoop.fs.{FileSystem, Path}
    val conf = new Configuration()

    val output = new Path(filepath)
    val hdfs = FileSystem.get(conf)

    // delete existing directory
    if (hdfs.exists(output)) {
      hdfs.delete(output, true)
    }
  }

  def checkpoint(df: Dataset[Row], checkpoint_dir: String): Dataset[Row] = {
    val path = checkpoint_dir + "/" + randomUUID() + ".ckpt"
    logInfo(s"Checkpoint to ${path}")
    df.write.parquet(path)
    if (curr_checkpoint_path != null) {
      delete_hdfs_file(curr_checkpoint_path)
    }
    curr_checkpoint_path = path
    df.sqlContext.read.parquet(path)
  }

  def make_clusters(df: Dataset[Row]): Dataset[Row] = {
    val cnts = df.groupBy("dest", "cid").agg(count("changed").alias("cnt"))
    val w = Window.partitionBy(col("dest")).orderBy(col("cnt").desc, col("cid"))

    val df2: DataFrame = cnts.withColumn("rn", row_number.over(w))
      .where(col("rn") === 1)
      .select("dest", "cid")
      .withColumnRenamed("dest", "node_id")
      .withColumnRenamed("cid", "new_cid")
    df2
  }

  def run_iteration(df: Dataset[Row]): Dataset[Row] = {
    import df.sparkSession.implicits._


    val df2 = make_clusters(df)

    val newdf = df.join(df2, $"src" === $"node_id")
      .withColumn("new_changed", !($"cid" === $"new_cid"))

    val newdf2 = newdf.select("src", "dest", "new_cid", "new_changed")
      .withColumnRenamed("new_cid", "cid")
      .withColumnRenamed("new_changed", "changed")

    return newdf2
  }

}

