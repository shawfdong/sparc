package net.sparc.graph

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer


class SparseBlockMatrix(rdd: RDD[(Int, Int, Float)], val n_row_block: Int, val n_col_block: Int,
                        val max_row: Int, val max_col: Int, val sparkSession: SparkSession)
  extends Serializable with LazyLogging {

  import sparkSession.implicits._

  val helper = new DCSCSparseMatrixHelper


  val bin_row = math.ceil(1.0 * max_row / n_row_block).toInt
  val bin_col = math.ceil(1.0 * max_col / n_col_block).toInt

  logger.info(s"#row_block=${n_row_block}, #col_bock=${n_col_block}, #row=${max_row}, #col=${max_col}, row_bin_size=${bin_row}, col_bin_size=${bin_col}")

  require(bin_row > 0)
  require(bin_col > 0)

  def row_index_to_block(i: Int) = {
    (i / bin_row, i % bin_row)
  }

  def col_index_to_block(i: Int) = {
    (i / bin_col, i % bin_col)
  }

  def fromCOO(bin_row: Int, bin_col: Int, tuples: Iterable[(Int, Int, Float)]) = {
    val lst = tuples.map(u => new COOItem(u._1, u._2, u._3)).to[ListBuffer]
    val matrix = helper.fromCOOItemArray(bin_row, bin_col, lst.asJava)
    helper.csc_to_case(matrix)
  }

  def getMatrix: Dataset[Row] = matrix

  private var matrix = {
    case class BlockItem(rowBlock: Int, rowInBlock: Int, colBlock: Int, colInBlock: Int, value: Float)
    val rdd2 = rdd.map {
      u =>
        val (rowBlock, rowInBlock) = row_index_to_block(u._1)
        val (colBlock, colInBlock) = col_index_to_block(u._2)
        BlockItem(rowBlock, rowInBlock, colBlock, colInBlock, u._3)
    }.groupBy(u => (u.rowBlock, u.colBlock)).map {
      u =>

        val m = this.fromCOO(bin_row, bin_col, u._2.map(v => (v.rowInBlock, v.colInBlock, v.value)))

        (u._1._1, u._1._2, m)
    }

    val df = rdd2.toDF("rowBlock", "colBlock", "value")
    df.printSchema()
    df.show(3)
    df
  }

  def show(n: Int = 5): Unit = {
    matrix.take(n).map {
      row =>
        val colBlock = row.getInt(0)
        val rowBlock = row.getInt(1)
        val subrow = row.getAs[Row](2)
        "bcol=" + colBlock + " rcol=" + rowBlock + " " + helper.makeString(subrow)
    }.foreach(println)
  }

  def get_row_num(block: Int, no: Int) = {
    bin_row * block + no
  }

  def get_col_num(block: Int, no: Int) = {
    bin_col * block + no
  }

  def argmax_along_row() = {

    val a = matrix.withColumn("aggval", udf_argmax_along_row($"value")).drop("value")
    //.select($"rowBlock",$"colBlock", map_keys($"aggval"), map_values($"aggval"))
    a.show()
    a.printSchema
    a.rdd.flatMap {
      case Row(rowBlock: Int, colBlock: Int, dict: Map[Int, Row]) =>
        dict.map {
          u =>
            val row_in_block = u._1
            val argmax_col_in_block = u._2.getInt(0)
            val argmax_val_in_block = u._2.getFloat(1)
            val i = get_row_num(rowBlock, row_in_block)
            val j = get_col_num(colBlock, argmax_col_in_block)
            (i, j, argmax_val_in_block)
        }
    }.groupBy(_._1).map {
      u =>
        val x = u._2.toSeq.sortBy(-_._3).head
        (x._1, x._2)
    }.toDF("node_id", "cluster_id")
  }

  def col_sum() = {
    matrix.rdd.map { row =>
      (row: @unchecked) match {
        case Row(rowBlock: Int, colBlock: Int,
        subrow: Row)
        =>
          val value = helper.row_to_wrapper(subrow)
          (colBlock, value)
      }
    }.reduceByKey {
      (u, v) =>
        helper.plus(u, v)
    }.toDF("colBlock", "colsum")
  }


  def udf_argmax_along_row = udf((m: Row) => {
    helper.argmax_along_row(m)
  })

  def udf_pow(r: Double) = udf((m: Row) => {
    helper.pow(m, r)
  })

  val udf_divide = udf((m1: Row, m2: Row) => {

    helper.divide(m1, m2)
  })

  val udf_mmult = udf((m1: Row, m2: Row) => {
    helper.mmult(m1, m2)
  })

  val udf_transpose = udf((m: Row) => {
    helper.transpose(m)
  })

  val udf_is_emtpy: UserDefinedFunction = udf((m: Row) => {
    !helper.isempty(m)
  })

  def compact() = {
    matrix = matrix.filter(udf_is_emtpy($"value"))
    this
  }

  def normalize_by_col() = {
    val colsum = col_sum()

    val df = matrix.join(colsum, Seq("colBlock")).withColumn("value", udf_divide($"value", $"colsum"))
    matrix = df.drop("colsum")
    this
  }


  def pow(r: Double) = {
    matrix = matrix.withColumn("value", udf_pow(r)($"value"))
    this
  }

  def transpose() = {
    matrix = matrix.withColumn("colBlock", $"rowBlock")
      .withColumn("rowBlock", $"colBlock")
      .withColumn("value", udf_transpose($"value"))
    this
  }

  def mmult(other: SparseBlockMatrix) = {
    val right = this.matrix
    val left = other.matrix
    matrix = right.join(left.select($"rowBlock", $"colBlock", $"value".alias("left_value"))
      , Seq("rowBlock", "colBlock")).withColumn("value", udf_mmult($"value", $"left_value"))
      .drop("left_value")
    this
  }

  def checkpointWith(checkpoint: Checkpoint, rm_prev_ckpt: Boolean) = {
    val (_, df) = checkpoint.checkpoint(this.matrix, rm_prev_ckpt)
    matrix = df
    this
  }
}

object SparseBlockMatrix {


  def from_rdd(rdd: RDD[(Int, Int, Float)], n_row_block: Int, n_col_block: Int, sparkSession: SparkSession): SparseBlockMatrix = {
    require(n_row_block > 0 && n_col_block > 0)
    val max_row = rdd.map(_._1).max()
    val max_col = rdd.map(_._2).max()
    new SparseBlockMatrix(rdd, n_row_block, n_col_block, max_row, max_col, sparkSession)
  }


}

object SparseBlockMatrixEncoder {

  implicit def CSCSparseMatrixEncoder: org.apache.spark.sql.Encoder[CSCSparseMatrix] =
    org.apache.spark.sql.Encoders.kryo[CSCSparseMatrix]

  implicit def DCSCSparseMatrixEncoder: org.apache.spark.sql.Encoder[DCSCSparseMatrix] =
    org.apache.spark.sql.Encoders.kryo[DCSCSparseMatrix]

  implicit def AbstractCSCSparseMatrixEncoder: org.apache.spark.sql.Encoder[AbstractCSCSparseMatrix] =
    org.apache.spark.sql.Encoders.kryo[AbstractCSCSparseMatrix]
}