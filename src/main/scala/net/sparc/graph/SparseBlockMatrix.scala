package net.sparc.graph

import breeze.linalg
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._


class SparseBlockMatrix(rdd: RDD[(Int, Int, Float)], val n_row_block: Int, val n_col_block: Int,
                        val max_row: Int, val max_col: Int, val sparkSession: SparkSession)
                        extends Serializable {
  def fromCOO(bin_row: Int, bin_col: Int, tuples: Iterable[(Int, Int, Float)]) = ???


  import sparkSession.implicits._


  val bin_row = math.ceil(1.0 * max_row / n_row_block).toInt
  val bin_col = math.ceil(1.0 * max_col / n_col_block).toInt
  require(bin_row > 0)
  require(bin_col > 0)

  def row_index_to_block(i: Int) = {
    (i / bin_row, i % bin_row)
  }

  def col_index_to_block(i: Int) = {
    (i / bin_col, i % bin_col)
  }

  def getMatrix: Dataset[Row] = matrix


  private var matrix = {
    case class BlockItem(rowBlock: Int, rowInBlock: Int, colBlock: Int, colInBlock: Int, value: Float)

    rdd.map {
      u =>
        val (rowBlock, rowInBlock) = row_index_to_block(u._1)
        val (colBlock, colInBlock) = col_index_to_block(u._2)
        BlockItem(rowBlock, rowInBlock, colBlock, colInBlock, u._3)
    }.groupBy(u => (u.rowBlock, u.colBlock)).map {
      u =>
        val m = this.fromCOO(bin_row,bin_col,u._2.map(v=>(v.rowInBlock,v.colInBlock,v.value)))

        (u._1._1, u._1._2, m)
    }.toDF("rowBlock", "colBlock", "value")
  }

  def col_sum() = {
    matrix.rdd.map {
      case Row(rowBlock: Int, colBlock: Int, value: AbstractCSCSparseMatrix) =>
        (colBlock, value)
    }.reduceByKey {
      (u, v) =>
        u.plus(v)
    }.toDF("colBlock", "colsum")
  }

  val udf_pow = udf((m: AbstractCSCSparseMatrix , r: Double) => {
    m.pow(r)
  })

  val udf_divide = udf((m1: AbstractCSCSparseMatrix , m2: AbstractCSCSparseMatrix ) => {
    m1.divide(m2)
  })

  val udf_mmult = udf((m1: AbstractCSCSparseMatrix , m2: AbstractCSCSparseMatrix ) => {
    m1.mmult(m2)
  })

  val udf_transpose = udf((m: AbstractCSCSparseMatrix ) => {
    m.transpose
  })

  def normalize_by_col() = {
    val colsum = col_sum()

    val df = matrix.join(colsum, Seq("colBlock")).withColumn("value", udf_divide($"value", $"colsum"))
    matrix = df.drop("colsum")
    this
  }


  def pow(r: Double) = {
    matrix = matrix.withColumn("value", udf_pow($"value"))
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
