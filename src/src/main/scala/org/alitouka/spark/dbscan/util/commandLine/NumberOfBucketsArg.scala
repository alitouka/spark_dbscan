package org.alitouka.spark.dbscan.util.commandLine

import org.alitouka.spark.dbscan.exploratoryAnalysis.ExploratoryAnalysisHelper

private [dbscan] trait NumberOfBucketsArg {
  var numberOfBuckets: Int = ExploratoryAnalysisHelper.DefaultNumberOfBucketsInHistogram
}
