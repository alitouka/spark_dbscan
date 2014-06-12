package org.alitouka.spark.dbscan.exploratoryAnalysis

import org.alitouka.spark.dbscan.spatial.Point

private [dbscan] class PointWithDistanceToNearestNeighbor (pt: Point, d: Double = Double.MaxValue) extends  Point (pt) {
  var distanceToNearestNeighbor = d
}
