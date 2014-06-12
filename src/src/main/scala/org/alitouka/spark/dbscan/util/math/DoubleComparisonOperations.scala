package org.alitouka.spark.dbscan.util.math


private [dbscan] class DoubleComparisonOperations (val originalValue: Double) {

  def ~~ (that: Double): Boolean = {
    isAlmostEqual(originalValue, that)
  }

  def >~ (that: Double): Boolean = {
    (originalValue > that) || isAlmostEqual(originalValue, that)
  }

  def <~ (that: Double): Boolean = {
    (originalValue < that) || isAlmostEqual(originalValue, that)
  }

  private def isAlmostEqual (x: Double, y: Double): Boolean = {
    Math.abs (x - y) <= DoubleComparisonOperations.Eps
  }
}

private [dbscan] object DoubleComparisonOperations {

  val Eps: Double = 1E-10

  implicit def doubleToDoubleComparisonOperations (x: Double): DoubleComparisonOperations = {
    new DoubleComparisonOperations (x)
  }
}
