package net.sparc.graph

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}

import scala.util.control.Breaks.{break, breakable}

class AffinityPropagation(val checkpoint_dir: String, val damping: Float, val convergence_iter: Int) extends LazyLogging {

  val checkpoint = new Checkpoint("AP", checkpoint_dir)

  private var convergence_count: Int = 0

  //return node,cluster pair
  def run(rawdf: Dataset[Row], sqlContext: SQLContext, max_iteration: Int): (RDD[(Int, Int)], Checkpoint) = {
    val spark = sqlContext.sparkSession

    require(max_iteration > 0, s"Maximum of steps must be greater than 0, but got ${max_iteration}")

    var (_, df) = checkpoint.checkpoint(rawdf.withColumn("availability", lit(0f))
      .withColumn("responsibility", lit(0f))
      .withColumn("cid", col("src")), true)
    logger.info("dataframe schema:")
    df.printSchema()
    df.show(10)
    breakable {

      for (i <- 1 to max_iteration) {
        df = checkpoint.checkpoint(run_iteration(df, i), true)._2
        if (convergence_count >= convergence_iter) {
          logger.info(s"Stop at iteration ${i}")
          break
        }
      }
    }

    (make_clusters(df).rdd.map(u => (u.getAs("node_id"), u.getAs("new_cid"))), checkpoint)
  }


  def make_clusters(df: Dataset[Row]): Dataset[Row] = {
    import df.sparkSession.implicits._

    val df1 = df.withColumn("s_plus_a", $"responsibility" + $"availability")


    val w = Window.partitionBy(col("src"))
      .orderBy(col("s_plus_a").desc, col("dest"))

    val df2: DataFrame = df1.withColumn("rn", row_number.over(w))
      .where(col("rn") === 1)
      .select("src", "dest")
      .withColumnRenamed("src", "node_id")
      .withColumnRenamed("dest", "new_cid")
    df2
  }

  def update_r(df: Dataset[Row]): Dataset[Row] = {
    import df.sparkSession.implicits._
    val rdd1 = df.select($"src", $"dest", $"similarity", $"availability").rdd.map {
      case Row(src: Int, dest: Int, similarity: Float, availability: Float) =>
        (src, dest, similarity, availability)
    }.groupBy(_._1).map { grouped =>
      def average(s: Seq[Float]): Double = s.foldLeft((0.0, 1))((acc, i) => ((acc._1 + (i - acc._1) / acc._2), acc._2 + 1))._1

      val table: Seq[(Int, Int, Float, Float)] = grouped._2.toList
      val s_k: Map[Int, Float] = table.map { x =>
        (x._2, x._3)
      }.groupBy(x => x._1).map(x => (x._1, average(x._2.map(_._2)).toFloat)).toMap
      val sorted_list = table.map { x =>
        (x._1, x._2, x._3 + x._4) //src, dest, similarity + availability
      }.sortBy(_._3)(Ordering[Float].reverse)
      if (sorted_list.length == 1) {
        sorted_list.map(x => {
          (x._1, x._2, s_k(x._2))
        })
      } else {
        val largest_sa = sorted_list(0)._3
        val largest_k = sorted_list(0)._2
        val second_largest_sa = sorted_list(1)._3

        sorted_list.map(x => {
          if (largest_k != x._2) (x._1, x._2, largest_sa) else (x._1, x._2, second_largest_sa)
        })
      }.map { x =>
        (x._1, x._2, s_k(x._2) - x._3)
      }
    }.flatMap(u => u)

    val df1 = rdd1.toDF("src", "dest", "new_responsibility")
    val df3: DataFrame = df.join(df1, Seq("src", "dest"))

    val df4 = df3.withColumn("responsibility", $"responsibility" * damping + $"new_responsibility" * (1.0f - damping))

    df4.drop("new_responsibility")

  }

  def update_a(df: Dataset[Row]): Dataset[Row] = {
    import df.sparkSession.implicits._
    val rdd1 = df.select($"src", $"dest", $"responsibility").rdd.map {
      case Row(src: Int, dest: Int, responsibility: Float) =>
        (src, dest, responsibility)
    }.groupBy(_._2).map { grouped =>

      def average(s: Seq[Float]): Double = s.foldLeft((0.0, 1))((acc, i) => ((acc._1 + (i - acc._1) / acc._2), acc._2 + 1))._1

      val table: Seq[(Int, Int, Float)] = grouped._2.toSeq
      val r_kk: Float = table.filter(u => u._2 == u._1).take(1)(0)._3

      val sum_max0_r_ik: Float = table.map { x => x._3 }
        .map(v => math.max(0, v)).sum

      val a_ik = table.map { case (i: Int, k: Int, r: Float) =>
        if (i == k) {
          (i, k, sum_max0_r_ik - math.max(0, r)) //here r is r_kk
        } else {
          (i, k, math.min(0, r_kk + sum_max0_r_ik - math.max(0, r_kk) - math.max(0, r)))
        }
      }

      a_ik

    }.flatMap(u => u)

    val df1 = rdd1.toDF("src", "dest", "new_availability")

    val df3: DataFrame = df.join(df1, Seq("src", "dest"))
    val df4 = df3.withColumn("availability", $"availability" * damping + $"new_availability" * (1.0f - damping))
    df4.drop("new_availability")

  }


  def run_iteration(df: Dataset[Row], iter: Int): Dataset[Row] = {
    import df.sparkSession.implicits._

    val df2 = update_r(df)
    val df3 = update_a(df2)


    val clusters = make_clusters(df3)

    val newdf = df3.join(clusters, $"src" === $"node_id")
    val cnt = newdf.select(sum(($"cid" =!= $"new_cid").cast("long"))).first.get(0)
    logger.info(s"${cnt} edges changed their clusters at iteration ${iter}")
    if (cnt == 0) {
      convergence_count += 1
    } else {
      convergence_count = 0
    }

    val newdf2 = newdf.select($"src", $"dest", $"similarity", $"responsibility", $"availability", $"new_cid")
      .withColumnRenamed("new_cid", "cid")
    newdf2
  }

}

object TupleUDFs {

  import org.apache.spark.sql.functions.udf
  // type tag is required, as we have a generic udf
  import scala.reflect.runtime.universe.{TypeTag, typeTag}

  def toTuple2[S: TypeTag, T: TypeTag] =
    udf[(S, T), S, T]((x: S, y: T) => (x, y))

  def toTuple4[S: TypeTag, T: TypeTag, U: TypeTag, V: TypeTag] =
    udf[(S, T, U, V), S, T, U, V]((x: S, y: T, u: U, v: V) => (x, y, u, v))
}

object AffinityPropagation extends LazyLogging {

  // Scala median function
  def median(inputList: List[Float]): Float = {
    val count = inputList.size
    if (count % 2 == 0) {
      val l = count / 2 - 1
      val r = l + 1
      (inputList(l) + inputList(r)).toFloat / 2
    } else
      inputList(count / 2).toFloat
  }

  //return node,cluster pair
  def run(edges: RDD[(Int, Int, Float)], sqlContext: SQLContext, max_iteration: Int, damping: Float,
          scaling: Float, convergence_iter: Int, checkpoint_dir: String): (RDD[(Int, Int)], Checkpoint) = {
    require(damping >= 0.5 && damping <= 1)
    val ap = new AffinityPropagation(checkpoint_dir, damping, convergence_iter)
    val spark = sqlContext.sparkSession

    require(max_iteration > 0, s"Maximum of steps must be greater than 0, but got ${max_iteration}")

    // add self linked edges, use the median as diagonal similarity
    val selfEdges =
      if (false) {
        edges.map(x => (x._1, x._3)).groupByKey()
          .mapValues(_.toList.sorted).map(m => {
          (m._1, m._1, median(m._2) * scaling)
        })
      } else {
        import sqlContext.implicits._
        val stat = edges.map(x => x._3).toDF("x").
          stat.approxQuantile("x", Array(1, 0.5, 0), 0.2)
        val maxs = stat(0).toFloat
        val med = stat(1).toFloat
        val mins = stat(2).toFloat
        logger.info(s"Median/max/min of similarity is ${med}/${maxs}/${mins}")
        edges.map(_._1).distinct.map(x => (x, x, med * scaling))
      }

    val edgeTuples = edges.union(selfEdges)
    val schema = new StructType()
      .add(StructField("src", IntegerType, false))
      .add(StructField("dest", IntegerType, false))
      .add(StructField("similarity", FloatType, false))
    val rawdf = spark.createDataFrame(edgeTuples.map { u => Row.fromTuple(u) }, schema)
    ap.run(rawdf, sqlContext, max_iteration)
  }

}

