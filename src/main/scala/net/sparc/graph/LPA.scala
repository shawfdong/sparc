package net.sparc.graph

import java.util.UUID.randomUUID

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}

import scala.util.control.Breaks.{break, breakable}

class LPA(val checkpoint_dir: String, val has_weight: Boolean) extends LazyLogging {

  val checkpoint = new Checkpoint("LPA", checkpoint_dir)

  //return node,cluster pair
  def run(rawdf: Dataset[Row], sqlContext: SQLContext, max_iteration: Int): (RDD[(Int, Int)], Checkpoint) = {
    val spark = sqlContext.sparkSession

    require(max_iteration > 0, s"Maximum of steps must be greater than 0, but got ${max_iteration}")

    var (_, df) = checkpoint.checkpoint(rawdf.withColumn("cid", column("src"))
      .withColumn("changed", lit(true)), true)

    logger.info("dataframe schema:")
    df.printSchema()
    df.show(10)
    breakable {

      for (i <- 1 to max_iteration) {
        df = checkpoint.checkpoint(run_iteration(df), true)._2
        //df.where($"changed" === true).show(10)

        val cnt = df.agg(sum(col("changed").cast("long"))).first.get(0)
        logger.info(s"${cnt} edges changed their clusters at iteration ${i}")
        if (cnt == 0) {
          logger.info(s"Stop at iteration ${i}")
          break
        }
      }
    }

    (make_clusters(df).rdd.map(u => (u.getAs("node_id"), u.getAs("new_cid"))), checkpoint)
  }


  def make_clusters(df: Dataset[Row]): Dataset[Row] = {
    val cnts =
      if (has_weight) {
        df.groupBy("dest", "cid").agg(sum("weight").alias("sumweight"))
      } else {
        df.groupBy("dest", "cid").agg(count("changed").alias("sumweight"))
      }
    val w = Window.partitionBy(col("dest"))
      .orderBy(col("sumweight").desc, col("cid"))

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

    val newdf2 = (if (!has_weight)
      newdf.select("src", "dest", "new_cid", "new_changed")
    else
      newdf.select("src", "dest", "weight", "new_cid", "new_changed")
      )
      .withColumnRenamed("new_cid", "cid")
      .withColumnRenamed("new_changed", "changed")

    newdf2
  }

}

object LPA extends LazyLogging {

  //return node,cluster pair
  def run_wo_weights(edges: RDD[(Int, Int)], sqlContext: SQLContext, max_iteration: Int,
                     checkpoint_dir: String): (RDD[(Int, Int)], Checkpoint) = {
    val lpa = new LPA(checkpoint_dir, false)
    val spark = sqlContext.sparkSession

    require(max_iteration > 0, s"Maximum of steps must be greater than 0, but got ${max_iteration}")

    // add self linked edges
    val selfEdges = edges.map(_._1).distinct().map(u => (u, u))
    val edgeTuples = edges.union(selfEdges)
    val schema = new StructType()
      .add(StructField("src", IntegerType, false))
      .add(StructField("dest", IntegerType, false))
    val rawdf = spark.createDataFrame(edgeTuples.map { u => Row.fromTuple(u) }, schema)
    lpa.run(rawdf, sqlContext, max_iteration)
  }

  //return node,cluster pair
  def run_with_weights(edges: RDD[(Int, Int, Float)], sqlContext: SQLContext, max_iteration: Int,
                       checkpoint_dir: String): (RDD[(Int, Int)], Checkpoint) = {
    val lpa = new LPA(checkpoint_dir, true)
    val spark = sqlContext.sparkSession

    require(max_iteration > 0, s"Maximum of steps must be greater than 0, but got ${max_iteration}")

    // add self linked edges
    val selfEdges = edges.map(_._1).distinct().map(u => (u, u, 1f))
    val edgeTuples = edges.union(selfEdges)
    val schema = new StructType()
      .add(StructField("src", IntegerType, false))
      .add(StructField("dest", IntegerType, false))
      .add(StructField("weight", FloatType, false))
    val rawdf = spark.createDataFrame(edgeTuples.map { u => Row.fromTuple(u) }, schema)
    lpa.run(rawdf, sqlContext, max_iteration)
  }

}
