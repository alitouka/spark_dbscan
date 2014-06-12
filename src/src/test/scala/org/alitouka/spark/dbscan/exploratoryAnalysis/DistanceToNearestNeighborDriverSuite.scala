package org.alitouka.spark.dbscan.exploratoryAnalysis

import org.alitouka.spark.dbscan.{DbscanSettings, SuiteBase, TestDatasets}
import org.alitouka.spark.dbscan.spatial.PointSortKey


class DistanceToNearestNeighborDriverSuite extends SuiteBase with TestDatasets {

  test ("DistanceToNearestNeighborDriver should work properly") {

    val it = dataset1_1.collect().map ( x => (new PointSortKey (x), x)).iterator
    val settings = new DbscanSettings ()
    val result = DistanceToNearestNeighborDriver.calculateDistancesToNearestNeighbors(it, settings.distanceMeasure)

    // TODO: add assertions

  }

}
