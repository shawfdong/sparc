package net.sparc.graph

import java.util

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.SparkConf
import org.scalatest.{FlatSpec, Matchers, _}
import breeze.linalg.{DenseMatrix => BDM}
import org.apache.spark.sql.Row

import scala.collection.JavaConverters._
import scala.util.Random
import org.apache.spark.sql.functions._

/**
  * Created by Lizhen Shi on 10/5/18.
  */
class SparseBlockMatrixSpec extends FlatSpec with Matchers with BeforeAndAfter with DataFrameSuiteBase {
  override def conf: SparkConf = {
    val conf = super.conf.set("spark.ui.enabled", "true")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrationRequired", "true")
    conf.registerKryoClasses(Array(classOf[AbstractCSCSparseMatrix],
      classOf[CSCSparseMatrix], classOf[DCSCSparseMatrix]))
    conf
  }

  "Block matrix" should "be convert to local and vice versa" in {
    import spark.implicits._
    (0 to TEST_ROUND).foreach { _ =>
      val smat1: DCSCSparseMatrix = createRandMatrix();
      val dim = (smat1.getNumRows, smat1.getNumCols)

      if (false) {
        println(dim)
        println(util.Arrays.toString(smat1.toArray))
      }

      val matrix = SparseBlockMatrix.from_local(smat1, bin_size = 20, spark)
      matrix.getMatrix.groupBy($"rowBlock", $"colBlock")
        .agg(count($"rowBlock").as("cnt")).select("cnt")
        .head.getLong(0) should be(1)

      matrix.getMatrix.select("value").rdd.map {
        vrow =>
          val row = vrow.getAs[Row](0)
          if (row.getAs[Int]("numRows") != matrix.bin_row
            || row.getAs[Int]("numCols") != matrix.bin_col) 1 else 0
      }.sum should be(0)

      matrix.getMatrix.select("value").rdd.map {
        vrow =>
          val row = vrow.getAs[Row](0)
          matrix.helper.row_to_csc(row).to_coo().asScala.map {
            x =>
              if (x.col >= matrix.bin_col || x.row >= matrix.bin_row) 1 else 0
          }.sum
      }.sum should be(0)

      matrix.getMatrix.select("value").rdd.map {
        vrow =>
          val row = vrow.getAs[Row](0)
          val a = matrix.helper.row_to_csc(row).to_coo().asScala.map {
            x =>
              (x.row, x.col)
          }
          a.length - a.toSet.size
      }.sum should be(0)


      val smat2 = matrix.to_local(dim = dim)

      val dsum = matrix.sum_abs

      println(smat1.sum_abs, dsum, smat2.sum_abs)
      dsum should be(smat2.sum_abs)
      dsum should be(smat1.sum_abs)

      check_array_equal(smat1, smat2)
    }
    //    Thread.sleep(1000 * 10000)
  }


  val TEST_ROUND = 5
  val N = 100

  def createRandMatrix(mn: (Int, Int) = null) = {
    val (m, n) = if (mn == null) {
      (Random.nextInt(N) + 100
        , Random.nextInt(N) + 100)
    } else {
      mn
    }
    var t = Random.nextInt(N) / N.toDouble;
    val bmatrix = BDM.rand[Double](m, n);
    for (i <- 1 until m)
      for (j <- 1 until n) {
        if (Random.nextInt(N) / N.toDouble > t) {
          bmatrix(i, j) = 0.0
        } else {
          bmatrix(i, j) = bmatrix(i, j).toFloat
        }
      }
    val arr = bmatrix.toArray.map(_.toFloat);
    val mat = DCSCSparseMatrix.from_array(bmatrix.rows, bmatrix.cols, arr)
    mat
  }


  def check_array_equal(smat: AbstractCSCSparseMatrix, bmat: AbstractCSCSparseMatrix, eps: Float = 1e-6f) = {
    smat.getNumRows should equal(bmat.getNumRows)
    smat.getNumCols should equal(bmat.getNumCols)
    val arr1 = smat.toArray
    val arr2 = bmat.toArray
    if (true) {
      println(util.Arrays.toString(arr1))
      println(util.Arrays.toString(arr2))
    }

    arr1.length should equal(arr2.length)
    for (i <- 0 until arr1.size) arr1(i) should be(arr2(i) +- eps)
  }
}
