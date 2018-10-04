package net.sparc.graph

import java.util

import org.apache.spark.sql.Row

import scala.collection.mutable

case class CSCMatrixWrapper(numRows: Int,
                            numCols: Int,
                            rowIndices: Array[Int],
                            values: Array[Float],
                            colPtrs: Array[Int]
                           )


class CSCSparseMatrixHelper extends Serializable {

  def row_to_wrapper(row: Row): CSCMatrixWrapper = {
    (row: @unchecked) match {
      case Row(numRows: Int, numCols: Int, colPtrs: mutable.WrappedArray[Int],
      rowIndices: mutable.WrappedArray[Int], values: mutable.WrappedArray[Float]) =>
        CSCMatrixWrapper(numRows, numCols, rowIndices.toArray, values.toArray, colPtrs.toArray)
    }
  }


  def row_to_csc(row: Row): CSCSparseMatrix = {
    (row: @unchecked) match {
      case Row(numRows: Int, numCols: Int, colPtrs: mutable.WrappedArray[Int],
      rowIndices: mutable.WrappedArray[Int], values: mutable.WrappedArray[Float]) =>
        new CSCSparseMatrix(numRows, numCols, colPtrs.toArray, rowIndices.toArray, values.toArray)
    }
  }


  def csc_to_case(mat: AbstractCSCSparseMatrix) = {
    CSCMatrixWrapper(mat.getNumRows, mat.getNumCols, mat.getRowIndices, mat.getValues, mat.getColPtrs)
  }

  def case_to_csc(mat: CSCMatrixWrapper) = {
    new CSCSparseMatrix(mat.numRows, mat.numCols, mat.colPtrs, mat.rowIndices, mat.values)
  }

  def transpose(m: Row): CSCMatrixWrapper = {
    val a = row_to_csc(m).transpose
    csc_to_case(a)
  }

  def mmult(m1: Row, m2: Row): CSCMatrixWrapper = {
    val a = row_to_csc(m1)
    val b = row_to_csc(m2)
    csc_to_case(a.mmult(b))
  }

  def divide(m1: Row, m2: Row): CSCMatrixWrapper = {
    val a = row_to_csc(m1)
    val b = row_to_csc(m2)
    csc_to_case(a.divide(b))
  }

  def pow(m: Row, r: Double): CSCMatrixWrapper = {
    val a = row_to_csc(m)
    csc_to_case(a.pow(r))
  }

  def plus(m1: Row, m2: Row): CSCMatrixWrapper = {
    val a = row_to_csc(m1)
    val b = row_to_csc(m2)
    csc_to_case(a.plus(b))
  }

  def plus(m1: CSCMatrixWrapper, m2: CSCMatrixWrapper): CSCMatrixWrapper = {
    val a = case_to_csc(m1)
    val b = case_to_csc(m2)
    csc_to_case(a.plus(b))
  }

  def fromCOOItemArray(numRows: Int, numCols: Int, lst: util.List[COOItem]): AbstractCSCSparseMatrix = {
    CSCSparseMatrix.fromCOOItemArray(numRows, numCols, lst)
  }
}

