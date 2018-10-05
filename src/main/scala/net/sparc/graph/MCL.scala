package net.sparc.graph

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}

import scala.util.Try
import scala.util.control.Breaks.{break, breakable}
import scala.collection.JavaConverters._

class MCL(val checkpoint_dir: String, val inflation: Float) extends LazyLogging {

  val checkpoint = new Checkpoint("MCL", checkpoint_dir)

  private var convergence_count: Int = 0

  //return node,cluster pair
  def run(rdd: RDD[(Int, Int, Float)], matrix_block_size: Int, sqlContext: SQLContext, max_iteration: Int,
          convergence_iter: Int): (RDD[(Int, Int)], Checkpoint) = {
    val spark = sqlContext.sparkSession

    require(max_iteration > 0, s"Maximum of steps must be greater than 0, but got ${max_iteration}")
    var sparseBlockMatrix = SparseBlockMatrix.from_rdd(rdd, bin_row = matrix_block_size, bin_col = matrix_block_size, spark)
      .normalize_by_col()
      .compact()
      .checkpointWith(checkpoint, rm_prev_ckpt = true)

    sparseBlockMatrix.getMatrix.printSchema()
    sparseBlockMatrix.getMatrix.show(10)
    var clusterdf: DataFrame = null
    breakable {

      for (i <- 1 to max_iteration) {
        logger.info(s"start running iteration ${i}")

        val ret = run_iteration(sparseBlockMatrix, clusterdf, i)
        sparseBlockMatrix = ret._1
        clusterdf = ret._2

        sparseBlockMatrix.show()
        clusterdf.show()
        if (convergence_count >= convergence_iter) {
          logger.info(s"Stop at iteration ${i}")
          break
        }
      }
    }

    (clusterdf.select("node_id", "cluster_id").rdd.map(u => (u.getInt(0), u.getInt(1))), checkpoint)
  }


  def make_clusters(sparseBlockMatrix: SparseBlockMatrix, clusterdf: Dataset[Row]) = {
    val df = sparseBlockMatrix.getMatrix
    import df.sparkSession.implicits._


    val df2 = sparseBlockMatrix.argmax_along_row()
    if (clusterdf == null) {
      df2.withColumn("old_cluster_id", $"node_id")
    } else {
      clusterdf.select($"node_id", $"cluster_id" as "old_cluster_id").join(df2, Seq("node_id"))
    }
  }

  def run_iteration(sparseBlockMatrix: SparseBlockMatrix,
                    clusterdf: DataFrame, iter: Int) = {
    val df = sparseBlockMatrix.getMatrix
    import df.sparkSession.implicits._

    val matrix2 = sparseBlockMatrix.mmult(sparseBlockMatrix)
    val matrix3 = matrix2.pow(2).normalize_by_col().compact.checkpointWith(checkpoint, rm_prev_ckpt = true)
    val (_, newclusterdf) = checkpoint.checkpoint(make_clusters(matrix3, clusterdf), rm_prev_ckpt = true, category = "cluster");
    val cnt = newclusterdf.select(sum(($"old_cluster_id" === $"cluster_id").cast("long"))).head().getLong(0)

    logger.info(s"${cnt} edges changed their clusters at iteration ${iter}")
    if (cnt > 0) {
      convergence_count = 0
    } else {
      convergence_count += 1
    }
    (matrix3, newclusterdf)
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
          sqlContext: SQLContext, max_iteration: Int, inflation: Float, convergence_iter: Int,
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

    mcl.run(edgeTuples, matrix_block_size, sqlContext, max_iteration, convergence_iter = convergence_iter)
  }

}

