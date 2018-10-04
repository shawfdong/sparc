package net.sparc.graph

import java.util

import org.apache.spark.sql.Row

import scala.collection.mutable

sealed trait SparseMatrixWrapper {
  def numRows: Int
  def numCols: Int
  def rowIndices: Array[Int]
  def values: Array[Float]
}

case class CSCMatrixWraper(override val numRows: Int,
                           override val numCols: Int,
                           override val  rowIndices: Array[Int],
                           override val  values: Array[Float],
                           colPtrs: Array[Int]
                          ) extends  SparseMatrixWrapper
case class DCSCMatrixWraper(override val numRows: Int,
                           override val numCols: Int,
                           override val  rowIndices: Array[Int],
                           override val  values: Array[Float],
                            colPtrPtrs: Array[Int], colPtrRepetition:Array[Int]
                           ) extends  SparseMatrixWrapper

abstract class SparseMatrixHelper{
  def transpose(m: Row):SparseMatrixWrapper

  def mmult(m1: Row, m2: Row): SparseMatrixWrapper

  def divide(m1: Row, m2: Row): SparseMatrixWrapper

  def pow(m: Row, r: Double): SparseMatrixWrapper

  def row_to_wrapper(subrow: Row): SparseMatrixWrapper


  def plus(m1: Row, m2: Row): SparseMatrixWrapper

  def plus(m1: SparseMatrixWrapper, m2: SparseMatrixWrapper): SparseMatrixWrapper


  def  fromCOOItemArray (numRows: Int, numCols: Int, lst: java.util.List[COOItem]): AbstractCSCSparseMatrix

  def csc_to_case(matrix: AbstractCSCSparseMatrix):SparseMatrixWrapper
}

class CSCSparseMatrixHelper extends  SparseMatrixHelper {

  def row_to_wrapper(row: Row): CSCMatrixWraper = {
    (row: @unchecked) match {
      case Row(numRows: Int, numCols: Int, colPtrs: mutable.WrappedArray[Int],
      rowIndices: mutable.WrappedArray[Int], values: mutable.WrappedArray[Float]) =>
        CSCMatrixWraper(numRows, numCols,  rowIndices.toArray, values.toArray, colPtrs.toArray)
    }
  }


  def row_to_csc(row: Row): CSCSparseMatrix = {
    (row: @unchecked) match {
      case Row(numRows: Int, numCols: Int, colPtrs: mutable.WrappedArray[Int],
      rowIndices: mutable.WrappedArray[Int], values: mutable.WrappedArray[Float]) =>
        new CSCSparseMatrix(numRows, numCols,colPtrs.toArray,  rowIndices.toArray, values.toArray)
    }
  }


  def csc_to_case(mat: AbstractCSCSparseMatrix) = {
    CSCMatrixWraper(mat.getNumRows, mat.getNumCols,  mat.getRowIndices, mat.getValues, mat.getColPtrs)
  }

  def case_to_csc(mat: CSCMatrixWraper) = {
    new CSCSparseMatrix(mat.numRows, mat.numCols, mat.colPtrs, mat.rowIndices, mat.values)
  }

  override def transpose(m: Row): SparseMatrixWrapper = {
    val a= row_to_csc(m).transpose
    csc_to_case(a)
  }

  override def mmult(m1: Row, m2: Row): SparseMatrixWrapper = {
    val a= row_to_csc(m1)
    val b= row_to_csc(m2)
    csc_to_case(a.mmult(b))
  }

  override def divide(m1: Row, m2: Row): SparseMatrixWrapper = {
    val a= row_to_csc(m1)
    val b= row_to_csc(m2)
    csc_to_case(a.divide(b))
  }

  override def pow(m: Row, r: Double): SparseMatrixWrapper = {
    val a= row_to_csc(m)
    csc_to_case(a.pow(r))
  }

  override def plus(m1: Row, m2: Row): SparseMatrixWrapper = {
    val a= row_to_csc(m1)
    val b= row_to_csc(m2)
    csc_to_case(a.plus(b))
  }

  override def plus(m1: SparseMatrixWrapper, m2: SparseMatrixWrapper): SparseMatrixWrapper = {
    val a= case_to_csc(m1.asInstanceOf[CSCMatrixWraper])
    val b= case_to_csc(m2.asInstanceOf[CSCMatrixWraper])
    csc_to_case(a.plus(b))
  }

  override def fromCOOItemArray(numRows: Int, numCols: Int, lst: util.List[COOItem]): AbstractCSCSparseMatrix =  {
    CSCSparseMatrix.fromCOOItemArray(numRows,numCols,lst)
  }
}


class DCSCSparseMatrixHelper extends  SparseMatrixHelper {
  def row_to_wrapper(row: Row): CSCMatrixWraper = {
    (row: @unchecked) match {
      case Row(numRows: Int, numCols: Int, colPtrs: mutable.WrappedArray[Int],
      rowIndices: mutable.WrappedArray[Int], values: mutable.WrappedArray[Float]) =>
        CSCMatrixWraper(numRows, numCols,  rowIndices.toArray, values.toArray, colPtrs.toArray)
    }
  }


  def row_to_csc(row: Row): CSCSparseMatrix = {
    (row: @unchecked) match {
      case Row(numRows: Int, numCols: Int, colPtrs: mutable.WrappedArray[Int],
      rowIndices: mutable.WrappedArray[Int], values: mutable.WrappedArray[Float]) =>
        new CSCSparseMatrix(numRows, numCols,colPtrs.toArray,  rowIndices.toArray, values.toArray)
    }
  }


  def csc_to_case(mat: AbstractCSCSparseMatrix) = {
    CSCMatrixWraper(mat.getNumRows, mat.getNumCols,  mat.getRowIndices, mat.getValues, mat.getColPtrs)
  }

  def case_to_csc(mat: CSCMatrixWraper) = {
    new CSCSparseMatrix(mat.numRows, mat.numCols, mat.colPtrs, mat.rowIndices, mat.values)
  }

  override def transpose(m: Row): SparseMatrixWrapper = {
    val a= row_to_csc(m).transpose
    csc_to_case(a)
  }

  override def mmult(m1: Row, m2: Row): SparseMatrixWrapper = {
    val a= row_to_csc(m1)
    val b= row_to_csc(m2)
    csc_to_case(a.mmult(b))
  }

  override def divide(m1: Row, m2: Row): SparseMatrixWrapper = {
    val a= row_to_csc(m1)
    val b= row_to_csc(m2)
    csc_to_case(a.divide(b))
  }

  override def pow(m: Row, r: Double): SparseMatrixWrapper = {
    val a= row_to_csc(m)
    csc_to_case(a.pow(r))
  }

  override def plus(m1: Row, m2: Row): SparseMatrixWrapper = {
    val a= row_to_csc(m1)
    val b= row_to_csc(m2)
    csc_to_case(a.plus(b))
  }

  override def plus(m1: SparseMatrixWrapper, m2: SparseMatrixWrapper): SparseMatrixWrapper = {
    val a= case_to_csc(m1.asInstanceOf[CSCMatrixWraper])
    val b= case_to_csc(m2.asInstanceOf[CSCMatrixWraper])
    csc_to_case(a.plus(b))
  }

  override def fromCOOItemArray(numRows: Int, numCols: Int, lst: util.List[COOItem]): AbstractCSCSparseMatrix =  {
    CSCSparseMatrix.fromCOOItemArray(numRows,numCols,lst)
  }
}
