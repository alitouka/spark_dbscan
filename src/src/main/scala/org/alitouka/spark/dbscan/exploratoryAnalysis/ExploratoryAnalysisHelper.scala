package org.alitouka.spark.dbscan.exploratoryAnalysis

import org.apache.spark.rdd.RDD
import  org.apache.spark.SparkContext._
import scala.collection.mutable.ArrayBuffer
import org.alitouka.spark.dbscan.DbscanSettings
import org.alitouka.spark.dbscan.spatial.DistanceAnalyzer
import org.apache.commons.math3.ml.distance.DistanceMeasure
import org.alitouka.spark.dbscan.util.io.IOHelper
import org.alitouka.spark.dbscan.spatial.rdd.PointsPartitionedByBoxesRDD
import org.alitouka.spark.dbscan.util.debug.Clock

/** Useful functions for exploratory analysis
 *
 */
object ExploratoryAnalysisHelper {

  private [dbscan] val DefaultNumberOfBucketsInHistogram = 16

  private [dbscan] implicit def toRDDWithLongIndexesAndDoubleValues (
    rddWithLongIndexesAndLongValues: RDD[(Long, Long)]): RDD[(Long, Double)] = {
    rddWithLongIndexesAndLongValues.map(x => (x._1, x._2.toDouble))
  }


  /** Calculates number of point's neighbors within a specified distance and builds a histogram
    * which represents distribution of this number across the dataset
    *
    * @param data Density-partitioned dataset
    * @param eps Distance
    * @param outputPath A path where the histogram is saved
    * @param numberOfBuckets Number of buckets in the histogram
    * @param distanceMeasure Distance measure class
    */
  def countNumberOfPointsWithinDistance (
      data: PointsPartitionedByBoxesRDD,
      eps: Double,
      outputPath: String,
      numberOfBuckets: Int = DefaultNumberOfBucketsInHistogram,
      distanceMeasure: DistanceMeasure = DbscanSettings.getDefaultDistanceMeasure): Unit = {

    val clock = new Clock ()

    val settings = new DbscanSettings()
      .withEpsilon(eps)
      .withDistanceMeasure(distanceMeasure)

    val distanceAnalyzer = new DistanceAnalyzer(settings)

    val closePoints = distanceAnalyzer.countClosePoints(data)

    val countsOfPointsWithNeighbors = closePoints
      .map(x => (x._1.pointId, x._2))
      .foldByKey(1L)( _+_ ) // +1 to include the point itself
      .cache()

    val indexedPoints = PointsPartitionedByBoxesRDD.extractPointIdsAndCoordinates (data)

    val countsOfPointsWithoutNeighbors = indexedPoints
      .keys
      .subtract(countsOfPointsWithNeighbors.keys)
      .map((_, 0L))

    val allCounts = countsOfPointsWithNeighbors union countsOfPointsWithoutNeighbors

    val histogram = ExploratoryAnalysisHelper.calculateHistogram(
      allCounts,
      numberOfBuckets)

    val triples = ExploratoryAnalysisHelper.convertHistogramToTriples(histogram)

    IOHelper.saveTriples(data.sparkContext.parallelize(triples), outputPath)

    clock.logTimeSinceStart("Calculation of number of points within " + eps)
  }


  private [dbscan] def calculateHistogram (
    pointIndexesWithValues: RDD[(Long, Double)]
  ): (Array[Double], Array[Long]) = {

    calculateHistogram(
      pointIndexesWithValues,
      ExploratoryAnalysisHelper.DefaultNumberOfBucketsInHistogram)
  }

  private [dbscan] def calculateHistogram (
    pointIndexesWithValues: RDD[(Long, Double)],
    bucketCount: Int): (Array[Double], Array[Long]) = {

    val data = keepOnlyValues(pointIndexesWithValues)

    ////////////////////////////////////////////////////////////////////////////////////
    //
    //  The following code was copied from DoubleRDDFunctions
    //
    //  Only the way how buckets are calculated was changed,
    //  because sometimes it results in IndexOutOfBoundsException
    //
    //  Consider the following example
    //  val min=1.8598046150064551E-6
    //  val max=0.22230930402715868
    //  val increment=0.05557686105563592
    //  val range = Range.Double.inclusive(min, max, increment)
    //  val buckets = range.toArray // An exception is thrown here
    //
    ////////////////////////////////////////////////////////////////////////////////////

    // Compute the minimum and the maxium
    val (max: Double, min: Double) = data.mapPartitions { items =>
      Iterator(items.foldRight(Double.NegativeInfinity,
        Double.PositiveInfinity)((e: Double, x: Pair[Double, Double]) =>
        (x._1.max(e), x._2.min(e))))
    }.reduce { (maxmin1, maxmin2) =>
      (maxmin1._1.max(maxmin2._1), maxmin1._2.min(maxmin2._2))
    }
    if (min.isNaN || max.isNaN || max.isInfinity || min.isInfinity ) {
      throw new UnsupportedOperationException(
        "Histogram on either an empty RDD or RDD containing +/-infinity or NaN")
    }
    val increment = (max-min)/bucketCount.toDouble
    var buckets: Array[Double] = null

    if (increment != 0) {

      val tempBuckets = new ArrayBuffer[Double]()
      var x = min

      while (x <= max) {
        tempBuckets += x
        x += increment
      }

      buckets = tempBuckets.toArray
    } else {
      buckets = Array (min, min)
    }

    (buckets, data.histogram(buckets, true))
  }

  private [dbscan] def convertHistogramToTriples (
    histogram: (Array[Double], Array[Long])): Seq[(Double, Double, Long)] = {

    val bucketBoundaries = histogram._1
    val values = histogram._2
    val result = new ArrayBuffer[(Double, Double, Long)] ()

    for (i <- 0 to bucketBoundaries.length-2) {
      val newTriple = (bucketBoundaries(i), bucketBoundaries(i+1), values(i))
      result += newTriple
    }

    result
  }


  private def keepOnlyValues (pointIndexesWithValues: RDD[(Long, Double)]) = {
    pointIndexesWithValues.map ( _._2 ).cache()
  }
}
