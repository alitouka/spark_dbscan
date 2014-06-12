package org.alitouka.spark.dbscan.util.commandLine

import org.alitouka.spark.dbscan.DbscanSettings

private [dbscan] trait EpsArg {
  var eps: Double = DbscanSettings.getDefaultEpsilon
}
