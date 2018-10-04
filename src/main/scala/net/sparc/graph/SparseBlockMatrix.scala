package net.sparc.graph

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer


case class CSCMatrixWraper(numRows: Int, numCols: Int, colPtrs: Array[Int], rowIndices: Array[Int], values: Array[Float]) {
  def plus(other: CSCMatrixWraper) = {
    val a = SparseBlockMatrix.case_to_csc(this).plus(SparseBlockMatrix.case_to_csc(other))
    SparseBlockMatrix.csc_to_case(a.asInstanceOf[CSCSparseMatrix])
  }

  def mmult(other: CSCMatrixWraper) = {
    val a = SparseBlockMatrix.case_to_csc(this).mmult(SparseBlockMatrix.case_to_csc(other))
    SparseBlockMatrix.csc_to_case(a.asInstanceOf[CSCSparseMatrix])
  }

  def divide(other: CSCMatrixWraper) = {
    val a = SparseBlockMatrix.case_to_csc(this).divide(SparseBlockMatrix.case_to_csc(other))
    SparseBlockMatrix.csc_to_case(a.asInstanceOf[CSCSparseMatrix])
  }

  def pow(r: Double) = {
    val a = SparseBlockMatrix.case_to_csc(this).pow(r)
    SparseBlockMatrix.csc_to_case(a.asInstanceOf[CSCSparseMatrix])
  }

  def transpose() = {
    val a = SparseBlockMatrix.case_to_csc(this).transpose()
    SparseBlockMatrix.csc_to_case(a.asInstanceOf[CSCSparseMatrix])
  }
}

class SparseBlockMatrix(rdd: RDD[(Int, Int, Float)], val n_row_block: Int, val n_col_block: Int,
                        val max_row: Int, val max_col: Int, val sparkSession: SparkSession, val sparse: String = "CSC")
  extends Serializable {


  def fromCOO(bin_row: Int, bin_col: Int, tuples: Iterable[(Int, Int, Float)]) = {

    val matrix = if (sparse.toUpperCase == "CSC") {
      val lst = tuples.map(u => new COOItem(u._1, u._2, u._3)).to[ListBuffer]
      CSCSparseMatrix.fromCOOItemArray(bin_row, bin_col, lst.asJava);
    } else {
      throw new Exception("Unknown " + sparse);
    }
    SparseBlockMatrix.csc_to_case(matrix);
  }


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

  def col_sum() = {
    matrix.rdd.map { row =>
      (row: @unchecked) match {
        case Row(rowBlock: Int, colBlock: Int,
        Row(numRows: Int, numCols: Int, colPtrs: mutable.WrappedArray[Int],
        rowIndices: mutable.WrappedArray[Int], values: mutable.WrappedArray[Float])) =>
          val value = CSCMatrixWraper(numRows, numCols, colPtrs.toArray, rowIndices.toArray, values.toArray)
          (colBlock, value)
      }
    }.reduceByKey {
      (u, v) =>
        u.plus(v)
    }.toDF("colBlock", "colsum")
  }


  def row_to_wrapper(row: Row): CSCMatrixWraper = {
    (row: @unchecked) match {
      case Row(numRows: Int, numCols: Int, colPtrs: mutable.WrappedArray[Int],
      rowIndices: mutable.WrappedArray[Int], values: mutable.WrappedArray[Float]) =>
        CSCMatrixWraper(numRows, numCols, colPtrs.toArray, rowIndices.toArray, values.toArray)
    }
  }

  def udf_pow(r: Double) = udf((m: Row) => {
    row_to_wrapper(m).pow(r)
  })

  val udf_divide = udf((m1: Row, m2: Row) => {

    row_to_wrapper(m1).divide(row_to_wrapper(m2))
  })

  val udf_mmult = udf((m1: Row, m2: Row) => {
    row_to_wrapper(m1).mmult(row_to_wrapper(m2))
  })

  val udf_transpose = udf((m: Row) => {
    row_to_wrapper(m).transpose
  })

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

  val csc_struct_type = StructType(
    List(
      StructField("numRow", IntegerType, false),
      StructField("numCols", IntegerType, false),
      StructField("colPtrs", ArrayType(IntegerType, false), false),
      StructField("rowIndices", ArrayType(IntegerType, false), false),
      StructField("values", ArrayType(IntegerType, false), false)

    )
  )

  def csc_to_case(mat: CSCSparseMatrix) = {
    CSCMatrixWraper(mat.getNumRows, mat.getNumCols, mat.getColPtrs, mat.getRowIndices, mat.getValues)
  }

  def case_to_csc(mat: CSCMatrixWraper) = {
    new CSCSparseMatrix(mat.numRows, mat.numCols, mat.colPtrs, mat.rowIndices, mat.values)
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