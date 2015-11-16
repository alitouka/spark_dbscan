package org.alitouka.spark.dbscan

import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.alitouka.spark.dbscan.util.io.IOHelper
import org.alitouka.spark.dbscan.util.commandLine._
import org.alitouka.spark.dbscan.spatial.rdd.PartitioningSettings
import org.alitouka.spark.dbscan.util.debug.{Troubleshooting, DebugHelper, Clock}

/** A driver program which runs DBSCAN clustering algorithm
 *
 */
object DbscanDriver extends Troubleshooting {

  private [dbscan] class Args (var minPts: Int = DbscanSettings.getDefaultNumberOfPoints,
      var borderPointsAsNoise: Boolean = DbscanSettings.getDefaultTreatmentOfBorderPoints)
      extends CommonArgs with EpsArg with NumberOfPointsInPartitionArg

  private [dbscan] class ArgsParser
    extends CommonArgsParser (new Args (), "DBSCAN clustering algorithm")
    with EpsArgParsing [Args]
    with NumberOfPointsInPartitionParsing [Args] {

    opt[Int] ("numPts")
      .required()
      .foreach { args.minPts = _ }
      .valueName("<minPts>")
      .text("TODO: add description")

    opt[Boolean] ("borderPointsAsNoise")
      .foreach { args.borderPointsAsNoise = _ }
      .text ("A flag which indicates whether border points should be treated as noise")
  }


  def main (args: Array[String]): Unit = {

    logEntry
    val argsParser = new ArgsParser ()

    if (argsParser.parse (args)) {

      val clock = new Clock ()


      val conf = new SparkConf()
        .setMaster(argsParser.args.masterUrl)
        .setAppName("DBSCAN")
        .setJars(Array(argsParser.args.jar))

      if (argsParser.args.debugOutputPath.isDefined) {
        conf.set (DebugHelper.DebugOutputPath, argsParser.args.debugOutputPath.get)
      }

      logDebug("Creating Spark context")
      val sc = new SparkContext(conf)

      logDebug("Reading raw data from '" + argsParser.args.inputPath + "'")
      val data = IOHelper.readDataset(sc, argsParser.args.inputPath)

      val settings = new DbscanSettings ()
        .withEpsilon(argsParser.args.eps)
        .withNumberOfPoints(argsParser.args.minPts)
        .withTreatBorderPointsAsNoise(argsParser.args.borderPointsAsNoise)
        .withDistanceMeasure(argsParser.args.distanceMeasure)

      val partitioningSettings = new PartitioningSettings (numberOfPointsInBox = argsParser.args.numberOfPoints)

      logDebug("Running clustering algorithm")
      val clusteringResult = Dbscan.train(data, settings, partitioningSettings)

      logDebug("Saving clustering result to '" + argsParser.args.outputPath + "'")
      IOHelper.saveClusteringResult(clusteringResult, argsParser.args.outputPath)

      clock.logTimeSinceStart("Clustering")
      logExit
    }
  }
}
