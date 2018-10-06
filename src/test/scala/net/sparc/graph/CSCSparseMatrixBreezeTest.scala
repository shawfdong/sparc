package net.sparc.graph

import util.Random
import java.util

import breeze.linalg.{*, Axis, DenseVector, Transpose, argmax, max, min, sum, DenseMatrix => BDM}
import breeze.numerics.{abs, pow}
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import scala.collection.JavaConverters._

class CSCSparseMatrixBreezeTest extends FunSuite {


  val TEST_ROUND = 200
  val N = 200

  def createRandMatrix(mn: (Int, Int) = null) = {
    val (m, n) = if (mn == null) {
      (Random.nextInt(N) + 1
        , Random.nextInt(N) + 1)
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
    val mat = CSCSparseMatrix.from_array(bmatrix.rows, bmatrix.cols, arr)
    (bmatrix, mat)
  }


  def check_array_equal(smat: AbstractCSCSparseMatrix, bmat: BDM[Double], filter_nan: Boolean = false, eps: Float = 1e-6f) = {
    smat.getNumRows should equal(bmat.rows)
    smat.getNumCols should equal(bmat.cols)
    val arr1 = smat.toArray
    val arr2 = if (filter_nan) {
      (bmat.toArray.map(u => if (u.isNaN || u.isInfinite) 0f else u.toFloat))
    } else {
      (bmat.toArray.map(_.toFloat))
    }
    if (false) {
      println(util.Arrays.toString(arr1))
      println(util.Arrays.toString(arr2.toArray))
    }
    //    implicit val custom = TolerantNumerics.tolerantFloatEquality(1e-6f)
    //    arr1 shouldBe (arr2)

    arr1.length should equal(arr2.length)
    for (i <- 0 until arr1.size) arr1(i) should be(arr2(i) +- eps)
  }

  test("test to coo") {
    (0 to TEST_ROUND).foreach { _ =>
      val (bmat1: BDM[Double], smat1: CSCSparseMatrix) = createRandMatrix();
      val dim = (smat1.getNumRows, smat1.getNumCols)
      if (false) {
        println(dim)
        println(util.Arrays.toString(bmat1.toArray.map(_.toFloat)))
        println(util.Arrays.toString(smat1.toArray))
      }
      val arr1 = smat1.toArray
      val arr2 = CSCSparseMatrix.fromCOOItemArray(dim._1, dim._2, Random.shuffle(smat1.to_coo.asScala).asJava).toArray
      arr1 should equal(arr2)

    }
  }

  test("test prune") {
    (0 to TEST_ROUND).foreach { _ =>
      val (bmat1: BDM[Double], smat1: CSCSparseMatrix) = createRandMatrix();
      val dim = (smat1.getNumRows, smat1.getNumCols)
      val maxv = max(bmat1)
      val minv = min(bmat1)
      val r = {
        val a = Random.nextInt(200) / 200.0
        minv + (maxv - minv) * a
      }

      bmat1(abs(bmat1) <:= r) := 0.0;

      if (false) {
        println(dim, r)
        println(util.Arrays.toString(bmat1.toArray.map(_.toFloat)))
        println(util.Arrays.toString(smat1.toArray))

      }


      check_array_equal(smat1.prune(r.toFloat), bmat1, eps = 1e-5f)
    }
  }

  test("test matrix mult") {
    (0 to TEST_ROUND).foreach { _ =>
      val (bmat1: BDM[Double], smat1: CSCSparseMatrix) = createRandMatrix();
      val dim = (smat1.getNumRows, smat1.getNumCols, Random.nextInt(N) + 1)
      val (bmat2: BDM[Double], smat2: CSCSparseMatrix) = createRandMatrix((dim._2, dim._3));
      if (false) {
        println(dim)
        println(util.Arrays.toString(bmat1.toArray.map(_.toFloat)))
        println(util.Arrays.toString(smat1.toArray))
        println(util.Arrays.toString(bmat2.toArray.map(_.toFloat)))
        println(util.Arrays.toString(smat2.toArray))
      }
      check_array_equal(smat1.mmult(smat2), bmat1 * bmat2, eps = 1e-2f)
    }
  }

  test("test power") {
    (0 to TEST_ROUND).foreach { _ =>
      val (bmat1: BDM[Double], smat1: CSCSparseMatrix) = createRandMatrix();
      val dim = (smat1.getNumRows, smat1.getNumCols)
      val r = {
        val a = Random.nextInt(200) / 17.0
        if (a == 0) 1.0 else a
      }
      if (false) {
        println(dim, r)
        println(util.Arrays.toString(bmat1.toArray.map(_.toFloat)))
        println(util.Arrays.toString(smat1.toArray))

      }

      check_array_equal(smat1.pow(r), pow(bmat1, r.toDouble), eps = 1e-3f)
    }
  }
  test("test argmax along row") {
    (0 to TEST_ROUND).foreach { _ =>
      val (bmat1: BDM[Double], smat1: CSCSparseMatrix) = createRandMatrix();
      val dim = (smat1.getNumRows, smat1.getNumCols)
      if (false) {
        println(dim)
        println(util.Arrays.toString(bmat1.toArray.map(_.toFloat)))
        println(util.Arrays.toString(smat1.toArray))
      }

      val d1 = smat1.argmax_along_row()
      val d2 = argmax(bmat1, Axis._1).toArray
      d2.indices.foreach {
        i =>
          if (d2(i) != 0) {
            d1.containsKey(i) should be(true)
            d1.get(i).x should equal(d2(i))
          }
      }
    }
  }
  test("test normalized by col") {
    (0 to TEST_ROUND).foreach { _ =>
      val (bmat1: BDM[Double], smat1: CSCSparseMatrix) = createRandMatrix();
      val dim = (smat1.getNumRows, smat1.getNumCols)
      val b = sum(bmat1, Axis._0)
      if (false) {
        println(dim, b.t.length)
        println(util.Arrays.toString(bmat1.toArray.map(_.toFloat)))
        println(util.Arrays.toString(smat1.toArray))

      }

      val c = bmat1(*, ::) / b.t
      check_array_equal(smat1.normalize_by_col(), c, eps = 1e-6f)
    }
  }

  test("test sum by col") {
    (0 to TEST_ROUND).foreach { _ =>
      val (bmat1: BDM[Double], smat1: CSCSparseMatrix) = createRandMatrix();
      val dim = (smat1.getNumRows, smat1.getNumCols)
      if (false) {
        println(dim)
        println(util.Arrays.toString(bmat1.toArray.map(_.toFloat)))
        println(util.Arrays.toString(smat1.toArray))

      }
      val b = sum(bmat1, Axis._0).t.asDenseMatrix
      check_array_equal(smat1.sum_by_col(), b, eps = 1e-2f)
    }
  }

  test("test transpose") {
    (0 to TEST_ROUND).foreach { _ =>
      val (bmat1: BDM[Double], smat1: CSCSparseMatrix) = createRandMatrix();
      val dim = (smat1.getNumRows, smat1.getNumCols)
      if (false) {
        println(dim)
        println(util.Arrays.toString(bmat1.toArray.map(_.toFloat)))
        println(util.Arrays.toString(smat1.toArray))

      }
      check_array_equal(smat1.transpose(), bmat1.t, eps = 1e-6f)
    }
  }

  test("test equal") {
    (0 to TEST_ROUND).foreach { _ =>
      val (bmat: BDM[Double], smat: CSCSparseMatrix) = createRandMatrix();
      check_array_equal(smat, bmat);
    }
  }

  test("test plus") {
    (0 to TEST_ROUND).foreach { _ =>
      val (bmat1: BDM[Double], smat1: CSCSparseMatrix) = createRandMatrix();
      val dim = (smat1.getNumRows, smat1.getNumCols)
      val (bmat2: BDM[Double], smat2: CSCSparseMatrix) = createRandMatrix(dim);
      if (false) {
        println(dim)
        println(util.Arrays.toString(bmat1.toArray.map(_.toFloat)))
        println(util.Arrays.toString(smat1.toArray))
        println(util.Arrays.toString(bmat2.toArray.map(_.toFloat)))
        println(util.Arrays.toString(smat2.toArray))
      }
      check_array_equal(smat1.plus(smat2), bmat1 + bmat2)
    }
  }

  test("test element wise multiplication") {
    (0 to TEST_ROUND).foreach { _ =>
      val (bmat1: BDM[Double], smat1: CSCSparseMatrix) = createRandMatrix();
      val dim = (smat1.getNumRows, smat1.getNumCols)
      val (bmat2: BDM[Double], smat2: CSCSparseMatrix) = createRandMatrix(dim);
      if (false) {
        println(dim)
        println(util.Arrays.toString(bmat1.toArray.map(_.toFloat)))
        println(util.Arrays.toString(smat1.toArray))
        println(util.Arrays.toString(bmat2.toArray.map(_.toFloat)))
        println(util.Arrays.toString(smat2.toArray))
      }
      check_array_equal(smat1.mult(smat2), bmat1 *:* bmat2)
    }
  }

  test("test element wise divide") {
    (0 to TEST_ROUND).foreach { _ =>
      val (bmat1: BDM[Double], smat1: CSCSparseMatrix) = createRandMatrix();
      val dim = (smat1.getNumRows, smat1.getNumCols)
      val (bmat2: BDM[Double], smat2: CSCSparseMatrix) = createRandMatrix(dim);
      if (false) {
        println(dim)
        println(util.Arrays.toString(bmat1.toArray.map(_.toFloat)))
        println(util.Arrays.toString(smat1.toArray))
        println(util.Arrays.toString(bmat2.toArray.map(_.toFloat)))
        println(util.Arrays.toString(smat2.toArray))
      }
      check_array_equal(smat1.divide(smat2), bmat1 /:/ bmat2, filter_nan = true, 1e-2f)
    }
  }


}
