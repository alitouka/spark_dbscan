package org.alitouka.spark.dbscan.util.commandLine

import org.alitouka.spark.dbscan.spatial.rdd.PartitioningSettings


private [dbscan] trait NumberOfPointsInPartitionArg {
  var numberOfPoints: Long = PartitioningSettings.DefaultNumberOfPointsInBox
}
