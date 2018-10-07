package net.sparc.graph

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{UserDefinedAggregateFunction, UserDefinedFunction}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, FloatType, StructField, StructType}

import scala.collection.JavaConverters._
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer


class SparseBlockMatrix(amatrix: DataFrame, coo_rdd: RDD[(Int, Int, Float)], val bin_row: Int, val bin_col: Int,
                        val num_row: Int, val num_col: Int, val sparkSession: SparkSession, val transposed: Boolean)
  extends Serializable with LazyLogging {

  def this(coo_rdd: RDD[(Int, Int, Float)], bin_row: Int, bin_col: Int,
           num_row: Int, num_col: Int, sparkSession: SparkSession) = {
    this(null, coo_rdd, bin_row, bin_col, num_row, num_col, sparkSession, transposed = false)
  }

  def this(amatrix: DataFrame, bin_row: Int, bin_col: Int,
           num_row: Int, num_col: Int, sparkSession: SparkSession, transposed: Boolean = false) = {
    this(amatrix, null, bin_row, bin_col, num_row, num_col, sparkSession, transposed)
  }

  import sparkSession.implicits._

  val helper = new DCSCSparseMatrixHelper


  val n_row_block = math.ceil(1.0 * num_row / bin_row).toInt
  val n_col_block = math.ceil(1.0 * num_col / bin_col).toInt


  logger.info(s"#row_block=${n_row_block}, #col_bock=${n_col_block}, #row=${num_row}, #col=${num_col}, row_bin_size=${bin_row}, col_bin_size=${bin_col}")

  require(bin_row > 0)
  require(bin_col > 0)
  require((amatrix == null || coo_rdd != null) || (amatrix != null || coo_rdd == null))

  val matrix: DataFrame = if (amatrix == null) {

    val row_index_to_block = (i: Int) => (i / bin_row, i % bin_row)
    val col_index_to_block = (i: Int) => (i / bin_col, i % bin_col)

    val f_row = if (transposed) col_index_to_block else row_index_to_block
    val f_col = if (transposed) row_index_to_block else col_index_to_block

    case class BlockItem(rowBlock: Int, rowInBlock: Int, colBlock: Int, colInBlock: Int, value: Float)
    val rdd2 = coo_rdd.map {
      u =>
        val (rowBlock, rowInBlock) = f_row(u._1)
        val (colBlock, colInBlock) = f_col(u._2)
        BlockItem(rowBlock = rowBlock, rowInBlock = rowInBlock, colBlock = colBlock,
          colInBlock = colInBlock, value = u._3)
    }.groupBy(u => (u.rowBlock, u.colBlock)).map {
      u =>

        val m = this.fromCOO(u._2.map(v => (v.rowInBlock, v.colInBlock, v.value)))

        (u._1._1, u._1._2, m)
    }

    val df = rdd2.toDF("rowBlock", "colBlock", "value")
    df.printSchema()
    df.show(3)
    df
  } else {
    amatrix
  }


  def fromCOO(tuples: Iterable[(Int, Int, Float)]) = {
    val lst = tuples.map(u => new COOItem(u._1, u._2, u._3)).to[ListBuffer]
    val matrix = helper.fromCOOItemArray(numRows = bin_row, numCols = bin_col, lst.asJava)
    helper.csc_to_case(matrix)
  }

  def getMatrix: Dataset[Row] = matrix


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
          val value = helper.row_to_csc(subrow).sum_by_col()
          (colBlock, value)
      }
    }.reduceByKey {
      (u, v) =>
        u.plus(v)
    }.map {
      u =>
        (u._1, helper.csc_to_case(u._2))
    }.toDF("colBlock", "colsum")
  }


  val udf_argmax_along_row = udf((m: Row) => {
    helper.argmax_along_row(m)
  })

  def udf_pow(r: Double) = udf((m: Row) => {
    helper.pow(m, r)
  })

  val udf_sum = udf((m: Row) => {
    helper.row_to_csc(m).sum
  })

  val udf_sum_abs = udf((m: Row) => {
    helper.row_to_csc(m).sum_abs
  })

  val udf_divide = udf((m1: Row, m2: Row) => {

    helper.divide(m1, m2)
  })

  val udf_plus = udf((m1: Row, m2: Row) => {

    helper.plus(m1, m2)
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
    val new_matrix = matrix.filter(udf_is_emtpy($"value"))
    new SparseBlockMatrix(new_matrix, bin_row, bin_col,
      num_row, num_col, sparkSession)
  }

  def normalize_by_col() = {
    val colsum = col_sum()

    val df = matrix.join(colsum, Seq("colBlock"))
      .withColumn("value", udf_divide($"value", $"colsum"))
    val new_matrix = df.drop("colsum")
    new SparseBlockMatrix(new_matrix, bin_row, bin_col,
      num_row, num_col, sparkSession)
  }


  def pow(r: Double) = {
    val new_matrix = matrix.withColumn("value", udf_pow(r)($"value"))
    new SparseBlockMatrix(new_matrix, bin_row, bin_col,
      num_row, num_col, sparkSession)

  }

  def sum(): Double = {
    matrix.select(org.apache.spark.sql.functions.sum(udf_sum($"value"))).head.getDouble(0)
  }

  def sum_abs(): Double = {
    matrix.select(org.apache.spark.sql.functions.sum(udf_sum_abs($"value"))).head.getDouble(0)
  }

  def transpose() = {
    val new_matrix = matrix.withColumn("prevRowBlock", $"rowBlock")
      .withColumn("rowBlock", $"colBlock")
      .withColumn("colBlock", $"prevRowBlock")
      .drop("prevRowBlock")
      .withColumn("value", udf_transpose($"value"))
      .select("rowBlock", "colBlock", "value")

    new SparseBlockMatrix(new_matrix, bin_col, bin_row,
      num_col, num_row, sparkSession, transposed = !transposed)

  }

  def plus(other: SparseBlockMatrix) = {
    require(this.num_col == other.num_col)
    require(this.num_row == other.num_row)
    require(this.bin_col == other.bin_col)
    require(this.bin_row == other.bin_row)

    val right = this.matrix
    val left = other.matrix
    val new_matrix = right.join(left.select($"rowBlock",
      $"colBlock", $"value".alias("left_value"))
      , Seq("rowBlock", "colBlock"), "outer")
      .withColumn("value", udf_plus($"value", $"left_value"))
      .drop("left_value").filter(udf_is_emtpy($"value"))
    new SparseBlockMatrix(new_matrix, this.bin_row, this.bin_col,
      this.num_row, this.num_col, sparkSession)
  }

  def mmult(other: SparseBlockMatrix) = {
    require(this.num_col == other.num_row)
    require(this.bin_col == other.bin_row)
    val right = this.matrix
    val left = other.matrix
    val new_matrix = right.join(left.select($"rowBlock".alias("colBlock"),
      $"colBlock".alias("left_colBlock"), $"value".alias("left_value"))
      , Seq("colBlock")).withColumn("value", udf_mmult($"value", $"left_value"))
      .select($"rowBlock", $"left_colBlock" as "colBlock", $"value").rdd
      .groupBy(x => (x.getAs[Int]("rowBlock"), x.getAs[Int]("colBlock")))
      .map { x =>
        val lst: Iterable[Row] = x._2.map(_.getAs[Row]("value"))
        (x._1._1, x._1._2, helper.plus(lst))
      }.toDF("rowBlock", "colBlock", "value")
      .filter(udf_is_emtpy($"value"))
    new SparseBlockMatrix(new_matrix, this.bin_row, other.bin_col,
      this.num_row, other.num_col, sparkSession)
  }

  def checkpointWith(checkpoint: Checkpoint, rm_prev_ckpt: Boolean) = {
    val (_, new_matrix) = checkpoint.checkpoint(this.matrix, rm_prev_ckpt)
    new SparseBlockMatrix(new_matrix, bin_row, bin_col,
      num_row, num_col, sparkSession)
  }

  def to_local(dim: (Int, Int) = null): AbstractCSCSparseMatrix = {
    val items = matrix.select($"rowBlock", $"colBlock", $"value").rdd.flatMap {
      case Row(rowBlock: Int, colBlock: Int, subrow: Row) =>
        val m = helper.row_to_csc(subrow)
        val lst = m.to_coo().asScala
        val cooItems = lst.map {
          v =>
            val i = get_row_num(rowBlock, v.row)
            val j = get_col_num(colBlock, v.col)
            new COOItem(i, j, v.v)
        }
        cooItems
    }.collect()

    val (m: Int, n: Int) = if (dim == null) {
      (num_row, num_col)
    } else {
      dim
    }
    helper.fromCOOItemArray(m, n, items.to[ListBuffer].asJava)
  }
}

object SparseBlockMatrix {
  def from_local(mat: DCSCSparseMatrix, bin_sizes: (Int, Int), spark: SparkSession) = {
    val dim = (mat.getNumRows, mat.getNumCols)
    val (row_bin, col_bin) = bin_sizes
    val a = mat.to_coo().asScala.map {
      u =>
        (u.row, u.col, u.v)
    }
    from_rdd(spark.sparkContext.parallelize(a), bin_row = row_bin, bin_col = col_bin, dim = null, spark)
  }


  def from_rdd(rdd: RDD[(Int, Int, Float)], bin_row: Int, bin_col: Int, dim: (Int, Int), sparkSession: SparkSession): SparseBlockMatrix = {
    require(bin_row > 0 && bin_col > 0)
    val (max_row: Int, max_col: Int) = if (dim == null) {
      val max1 = rdd.map(_._1).max() + 1
      val max2 = rdd.map(_._2).max() + 1
      (max1, max2)
    } else {
      dim
    }
    new SparseBlockMatrix(rdd, bin_row = bin_row, bin_col = bin_col, num_row = max_row, num_col = max_col, sparkSession)
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

