package org.alitouka.spark.dbscan

import org.apache.spark.SparkContext

import org.apache.spark.SparkContext._
import scala.Some
import org.alitouka.spark.dbscan.spatial.{PointSortKey, Point}
import scala.collection.mutable.WrappedArray.ofDouble
import org.alitouka.spark.dbscan.util.debug.Clock

class DbscanSuiteBase extends SuiteBase with TestDatasets {

  var clock: Clock = _

  override def beforeEach() = {
    super.beforeEach ()

    clock = new Clock ()
  }

  override def afterEach () = {
    clock.logTimeSinceStart()
  }

  def groupPointsByCluster (clusteringResult: DbscanModel): Array [Iterable[PointCoordinates]] = {
    val clock = new Clock ()
    val result = clusteringResult.clusteredPoints.map ( x => (x.clusterId, x.coordinates)).groupByKey().map ( _._2 ).collect ()
    clock.logTimeSinceStart("Grouping points by cluster took")

    result
  }

  def groupPointsByCluster (it: Iterator[(PointSortKey, Point)]): Array[Array[Point]] = {

    it.map (_._2).toArray.groupBy(_.clusterId).map(_._2).toArray
  }

  def findClusterWithPoint (clusters: Array[Iterable[ofDouble]], pt: Point): Option[Iterable[ofDouble]] = {

    val filteredClusters = clusters.filter( _.toArray.contains(pt.coordinates) )

    filteredClusters.size match {
      case 0 => None
      case _ => Some(filteredClusters(0))
    }
  }

  def findClusterWithPoint2 (clusters: Array[Array[Point]], pt: Point): Option[Array [Point]] = {

    val filteredClusters = clusters.filter( _.contains(pt) )

    filteredClusters.size match {
      case 0 => None
      case _ => Some(filteredClusters(0))
    }
  }
}
