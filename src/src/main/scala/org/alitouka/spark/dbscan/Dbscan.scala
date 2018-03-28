package org.alitouka.spark.dbscan

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.commons.math3.ml.distance.{EuclideanDistance, DistanceMeasure}
import scala.reflect._
import org.apache.spark.internal.Logging
import org.alitouka.spark.dbscan.spatial.DistanceAnalyzer
import org.alitouka.spark.dbscan.spatial.rdd.PartitioningSettings

/** Base class for implementations of the DBSCAN algorithm
  *
  * @constructor Creates a new instance of a particular implementation with specified settings
  *
  * @param settings Parameters of the algorithm. See [[org.alitouka.spark.dbscan.DbscanSettings]]
  *                 for details
  */
abstract class Dbscan protected (
  protected val settings: DbscanSettings,
  protected val partitioningSettings: PartitioningSettings = new PartitioningSettings ())
  extends Serializable
  with Logging {

  protected val distanceAnalyzer = new DistanceAnalyzer(settings)

  protected def run (data: RawDataSet): DbscanModel
}

/** Serves as a factory for objects which implement the DBSCAN algorithm
  * and provides a convenience method for starting the algorithm
  *
  */
object Dbscan {

  /** Instantiates an object which implements the DBSCAN algorithm
    *
    * @param settings Parameters of the algorithm. See
    *                 [[org.alitouka.spark.dbscan.DbscanSettings]] for details
    * @return A new object which implements the DBSCAN algorithm
    */
  protected def apply (settings: DbscanSettings,
    partitioningSettings: PartitioningSettings = new PartitioningSettings ()): Dbscan = {

    new DistributedDbscan(settings, partitioningSettings)
  }

  /** A convenience method which you can use to start clustering
    *
    * @param data A [[org.alitouka.spark.dbscan.RawDataSet]] which should be clustered
    * @param settings Parameters of the algorithm. See
    *                 [[org.alitouka.spark.dbscan.DbscanSettings]] for details
    * @return A [[org.alitouka.spark.dbscan.DbscanModel]] populated with information about clusters
    */
  def train (data: RawDataSet, settings: DbscanSettings,
    partitioningSettings: PartitioningSettings = new PartitioningSettings ()): DbscanModel = {
    Dbscan (settings, partitioningSettings).run (data)
  }

  private [dbscan] def keepOnlyPairsWithKeys [K, V] (pairs: RDD[(K, V)], keysToLeave: RDD[K])
    (implicit arg0: ClassTag[K], arg1: ClassTag[V]) = {

    keysToLeave.map ( (_, null) ).join (pairs).map ( x => (x._1, x._2._2) )
  }



}
