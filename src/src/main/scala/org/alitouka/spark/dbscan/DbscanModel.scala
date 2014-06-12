package org.alitouka.spark.dbscan

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.alitouka.spark.dbscan.spatial.{Point, DistanceAnalyzer}
import org.alitouka.spark.dbscan.spatial.rdd.PointsPartitionedByBoxesRDD

/** Represents results calculated by DBSCAN algorithm.
  *
  * This object is returned from [[org.alitouka.spark.dbscan.Dbscan.run]] method.
  * You cannot instantiate it directly
  */
class DbscanModel private[dbscan] (val allPoints: RDD[Point],
val settings: DbscanSettings)
  extends Serializable {



  /** Predicts which cluster a point would belong to
    *
    * @param newPoint A [[org.alitouka.spark.dbscan.PointCoordinates]] for which you want to make a prediction
    * @return If the point can be assigned to a cluster, then this cluster's ID is returned.
    *         If the point appears to be a noise point, then
    *         [[org.alitouka.spark.dbscan.DbscanModel.NoisePoint]] is returned.
    *         If the point is surrounded by so many noise points that they can constitute a new
    *         cluster, then [[org.alitouka.spark.dbscan.DbscanModel.NewCluster]] is returned
    */
  def predict (newPoint: Point): ClusterId = {
    val distanceAnalyzer = new DistanceAnalyzer(settings)
    val neighborCountsByCluster = distanceAnalyzer.findNeighborsOfNewPoint(allPoints, newPoint.coordinates)
      .map ( x => (x.clusterId, x) )
      .countByKey()

    val neighborCountsWithoutNoise = neighborCountsByCluster.filter(_._1 != DbscanModel.NoisePoint)
    val possibleClusters = neighborCountsWithoutNoise.filter(_._2 >= settings.numberOfPoints-1)
    val noisePointsCount = if (neighborCountsByCluster.contains(DbscanModel.NoisePoint)) {
      neighborCountsByCluster (DbscanModel.NoisePoint)
    }
    else {
      0L
    }

    if (possibleClusters.size >= 1) {

      // If a point is surrounded by >= numPts points which belong to one cluster, then the point should be assigned to that cluster
      // If there are more than one clusters, then the cluster will be chosen arbitrarily

      possibleClusters.keySet.head
    }
    else if (neighborCountsWithoutNoise.size >= 1 && !settings.treatBorderPointsAsNoise) {

      // If there is not enough surrounding points, then the new point is a border point of a cluster
      // In this case, the prediction depends on treatBorderPointsAsNoise flag.
      // If it allows assigning border points to clusters, then the new point will be assigned to the cluster
      // If there are many clusters, then one of them will be chosen arbitrarily

      neighborCountsWithoutNoise.keySet.head
    }
    else if (noisePointsCount >= settings.numberOfPoints-1) {

      // The point is surrounded by sufficiently many noise points so that together they will constitute a new cluster

      DbscanModel.NewCluster
    }
    else {

      // If none of the above conditions are met, then the new point is noise

      DbscanModel.NoisePoint
    }
  }


  /** Returns only noise points
    *
    * @return
    */
  def noisePoints: RDD[Point] = { allPoints.filter(_.clusterId == DbscanModel.NoisePoint) }

  /** Returns points which were assigned to clusters
    *
    * @return
    */
  def clusteredPoints: RDD[Point] = { allPoints.filter( _.clusterId != DbscanModel.NoisePoint) }
}

/** Contains constants which designate cluster ID
  *
  */
object DbscanModel {

  /** Designates noise points
    *
    */
  val NoisePoint: ClusterId = 0

  /** Indicates that a new cluster would appear in a [[org.alitouka.spark.dbscan.DbscanModel]] if
    *  a new point was added to it
    */
  val NewCluster: ClusterId = -1

  /** Initial value for cluster ID of each point.
    *
    */
  private [dbscan] val UndefinedCluster: ClusterId = -2
}
