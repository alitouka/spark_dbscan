package org.alitouka.spark.dbscan.spatial

import org.apache.commons.math3.ml.distance.DistanceMeasure
import org.alitouka.spark.dbscan._

private [dbscan] trait DistanceCalculation {

  protected def calculateDistance (pt1: Point, pt2: Point)(implicit distanceMeasure: DistanceMeasure): Double = {
    calculateDistance (pt1.coordinates, pt2.coordinates)
  }

  protected def calculateDistance (pt1: PointCoordinates, pt2: PointCoordinates)
    (implicit distanceMeasure: DistanceMeasure): Double = {

    distanceMeasure.compute (pt1.toArray, pt2.toArray)
  }

  protected def isPointCloseToAnyBound (pt: Point, box: Box, threshold: Double): Boolean = {

    (0 until pt.coordinates.size).exists( i => isPointCloseToBound (pt, box.bounds(i), i, threshold))
  }

  protected def isPointCloseToBound (pt: Point, bound: BoundsInOneDimension, dimension: Int, threshold: Double)
    :Boolean = {

    // It will work for Euclidean or Manhattan distance measure but may not work for others
    // TODO: generalize for different distance measures

    val x = pt.coordinates(dimension)
    Math.abs(x - bound.lower) <= threshold || Math.abs(x - bound.upper) <= threshold
  }
}
