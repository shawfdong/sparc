package net.sparc.graph

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}

import scala.util.control.Breaks.{break, breakable}

class MCL(val checkpoint_dir: String, val inflation: Float) extends LazyLogging {

  val checkpoint = new Checkpoint("MCL", checkpoint_dir)

  private var convergence_count: Int = 0

  //return node,cluster pair
  def run(rdd: RDD[(Int, Int, Float)], matrix_block_size: Int, sqlContext: SQLContext, max_iteration: Int): (RDD[(Int, Int)], Checkpoint) = {
    val spark = sqlContext.sparkSession

    require(max_iteration > 0, s"Maximum of steps must be greater than 0, but got ${max_iteration}")
    var sparseBlockMatrix = SparseBlockMatrix.from_rdd(rdd, matrix_block_size, matrix_block_size, spark).normalize_by_col()
      .checkpointWith(checkpoint, rm_prev_ckpt = true)


    sparseBlockMatrix.getMatrix.printSchema()
    sparseBlockMatrix.getMatrix.show(10)
    breakable {

      for (i <- 1 to max_iteration) {
        logger.info(s"start running iteration ${i}")

        sparseBlockMatrix = run_iteration(sparseBlockMatrix, i).checkpointWith(checkpoint, rm_prev_ckpt = true)
        if (convergence_count >= 1) {
          logger.info(s"Stop at iteration ${i}")
          break
        }
        if (i > 3) {
          throw new Exception("ADDF");
        }
      }
    }

    (make_clusters(sparseBlockMatrix).rdd.map(u => (u.getAs("node_id"), u.getAs("new_cid"))), checkpoint)
  }


  def make_clusters(sparseBlockMatrix: SparseBlockMatrix): Dataset[Row] = {
    val df = sparseBlockMatrix.getMatrix
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

  def run_iteration(sparseBlockMatrix: SparseBlockMatrix, iter: Int) = {
    val df = sparseBlockMatrix.getMatrix
    val matrix2 = sparseBlockMatrix.mmult(sparseBlockMatrix.transpose())
    val matrix3 = matrix2.pow(2).normalize_by_col()
    matrix3
  }

}

object MCL extends LazyLogging {

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
  def run(edges: RDD[(Int, Int, Float)], matrix_block_size: Int,
          sqlContext: SQLContext, max_iteration: Int, inflation: Float,
          scaling: Float, checkpoint_dir: String): (RDD[(Int, Int)], Checkpoint) = {
    require(inflation > 0)
    require(matrix_block_size > 0)
    val mcl = new MCL(checkpoint_dir, inflation)
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

    mcl.run(edgeTuples, matrix_block_size, sqlContext, max_iteration)
  }

}

