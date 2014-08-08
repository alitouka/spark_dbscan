package org.alitouka.spark

import org.apache.spark.rdd.RDD
import scala.collection.mutable.WrappedArray.ofDouble
import org.apache.commons.math3.ml.distance.EuclideanDistance
import org.alitouka.spark.dbscan.spatial.{PointSortKey, Point}

/** Contains implementation of distributed DBSCAN algorithm as well as tools for exploratory analysis.
  *
  *
  */
package object dbscan {

  /** Represents one record in a dataset
    *
    */
  type PointCoordinates = ofDouble

  /** Represents a dataset which needs to be clustered
    *
    */
  type RawDataSet = RDD[Point]

  /** Unique point ID in a data set
   *
   */
  private [dbscan] type PointId = Long

  private [dbscan] type TempPointId = Int

  /** Unique id of a box-shaped region in a data set
   *
   */
  private [dbscan] type BoxId = Int

  /** Cluster ID
   *
   */
  type ClusterId = Long

  /** A pair of IDs of density-based partitions adjacent to each other
   *
   */
  private [dbscan] type PairOfAdjacentBoxIds = (BoxId, BoxId)
}
