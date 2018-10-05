package net.sparc.graph

import java.util

import org.apache.spark.sql.{ColumnName, Row}
import scala.collection.JavaConverters._

import scala.collection.mutable

case class DCSCMatrixWrapper(numRows: Int,
                             numCols: Int,
                             rowIndices: Array[Int],
                             values: Array[Float],
                             colPtrPtrs: Array[Int], colPtrRepetition: Array[Int]
                            )


class DCSCSparseMatrixHelper extends Serializable {


  def row_to_wrapper(row: Row): DCSCMatrixWrapper = {
    (row: @unchecked) match {
      case Row(numRows: Int, numCols: Int,
      rowIndices: mutable.WrappedArray[Int], values: mutable.WrappedArray[Float],
      colPtrPtrs: mutable.WrappedArray[Int], colPtrRepetition: mutable.WrappedArray[Int]) =>
        DCSCMatrixWrapper(numRows, numCols, rowIndices.toArray, values.toArray, colPtrPtrs.toArray, colPtrRepetition.toArray)
    }
  }

  def argmax_along_row(m: Row):Map[Integer,(Integer,Float)] ={
    row_to_csc(m).argmax_along_row.asScala.map{
      u=>
        (u._1,(u._2.x,u._2.y.toFloat))
    }.toMap
  }

  def row_to_csc(row: Row): DCSCSparseMatrix = {
    (row: @unchecked) match {
      case Row(numRows: Int, numCols: Int,
      rowIndices: mutable.WrappedArray[Int], values: mutable.WrappedArray[Float],
      colPtrPtrs: mutable.WrappedArray[Int], colPtrRepetition: mutable.WrappedArray[Int]) =>
        new DCSCSparseMatrix(numRows, numCols, colPtrPtrs.toArray, colPtrRepetition.toArray, rowIndices.toArray, values.toArray)
    }
  }

  def makeString(row: Row) = {
    row_to_csc(row).toString
  }

  def isempty(row: Row) = {
    (row: @unchecked) match {
      case Row(_: Int, _: Int,
      _: mutable.WrappedArray[Int], values: mutable.WrappedArray[Float],
      _: mutable.WrappedArray[Int], _: mutable.WrappedArray[Int]) =>
        values.length == 0
    }
  }

  def csc_to_case(mat0: AbstractCSCSparseMatrix) = {
    val mat = mat0.asInstanceOf[DCSCSparseMatrix]
    DCSCMatrixWrapper(mat.getNumRows, mat.getNumCols, mat.getRowIndices, mat.getValues, mat.getColPtrPtrs, mat.getColPtrRepetition)
  }

  def case_to_csc(mat: DCSCMatrixWrapper) = {
    new DCSCSparseMatrix(mat.numRows, mat.numCols, mat.colPtrPtrs, mat.colPtrRepetition, mat.rowIndices, mat.values)
  }

  def transpose(m: Row): DCSCMatrixWrapper = {
    val a = row_to_csc(m).transpose
    csc_to_case(a)
  }

  def mmult(m1: Row, m2: Row): DCSCMatrixWrapper = {
    val a = row_to_csc(m1)
    val b = row_to_csc(m2)
    csc_to_case(a.mmult(b))
  }

  def divide(m1: Row, m2: Row): DCSCMatrixWrapper = {
    val a = row_to_csc(m1)
    val b = row_to_csc(m2)
    csc_to_case(a.divide(b))
  }

  def pow(m: Row, r: Double): DCSCMatrixWrapper = {
    val a = row_to_csc(m)
    csc_to_case(a.pow(r))
  }

  def plus(m1: Row, m2: Row): DCSCMatrixWrapper = {
    val a = row_to_csc(m1)
    val b = row_to_csc(m2)
    csc_to_case(a.plus(b))
  }

  def plus(m1: DCSCMatrixWrapper, m2: DCSCMatrixWrapper): DCSCMatrixWrapper = {
    val a = case_to_csc(m1.asInstanceOf[DCSCMatrixWrapper])
    val b = case_to_csc(m2.asInstanceOf[DCSCMatrixWrapper])
    csc_to_case(a.plus(b))
  }

  def fromCOOItemArray(numRows: Int, numCols: Int, lst: util.List[COOItem]): AbstractCSCSparseMatrix = {
    DCSCSparseMatrix.fromCOOItemArray(numRows, numCols, lst)
  }
}
