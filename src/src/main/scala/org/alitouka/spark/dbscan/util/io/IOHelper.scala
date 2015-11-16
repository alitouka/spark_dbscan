package org.alitouka.spark.dbscan.util.io

import org.apache.spark.{Logging, SparkContext}
import scala.collection.mutable.WrappedArray.ofDouble
import org.alitouka.spark.dbscan.{DbscanModel, RawDataSet, ClusterId, PointCoordinates}
import org.apache.spark.rdd.RDD
import org.alitouka.spark.dbscan.spatial.Point
import org.alitouka.spark.dbscan.util.debug.{Troubleshooting, Clock}

/** Contains functions for reading and writing data
  *
  */
object IOHelper extends Troubleshooting {

  /** Reads a dataset from a CSV file. That file should contain double values separated by commas
    *
    * @param sc A SparkContext into which the data should be loaded
    * @param path A path to the CSV file
    * @return A [[org.alitouka.spark.dbscan.RawDataSet]] populated with points
    */
  def readDataset (sc: SparkContext, path: String): RawDataSet = {

    logEntry
    val clock = new Clock ()

    logDebug("Reading raw data from '" + path + "'")
    val rawData = sc.textFile (path)

    logDebug("Creating a point for each line in an input file")
    val result = rawData.map (
      line => {
        new Point (line.split(separator).map( _.toDouble ))
      }
    )

    clock.logTimeSinceStart("Reading dataset")
    logExit

    result
  }

  /** Saves clustering result into a CSV file. The resulting file will contain the same data as the input file,
    * with a cluster ID appended to each record. The order of records is not guaranteed to be the same as in the
    * input file
    *
    * @param model A [[org.alitouka.spark.dbscan.DbscanModel]] obtained from Dbscan.train method
    * @param outputPath Path to a folder where results should be saved. The folder will contain multiple
    *                   partXXXX files
    */
  def saveClusteringResult (model: DbscanModel, outputPath: String) {

    logEntry

    model.allPoints.map ( pt => {

      pt.coordinates.mkString(separator) + separator + pt.clusterId
    } ).saveAsTextFile(outputPath)

    logExit
  }

  private [dbscan] def saveTriples (data: RDD[(Double, Double, Long)], outputPath: String) {
    data.map ( x => x._1 + separator + x._2 + separator + x._3 ).saveAsTextFile(outputPath)
  }

  private def separator = ","

}
