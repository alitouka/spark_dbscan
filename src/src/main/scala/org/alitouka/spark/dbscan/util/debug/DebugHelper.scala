package org.alitouka.spark.dbscan.util.debug

import org.apache.spark.SparkContext
import org.apache.hadoop.fs.Path


private [dbscan] object DebugHelper {

  def doAndSaveResult (sc: SparkContext, relativePath: String)(fn: String => Unit) {

    val opt = sc.getConf.getOption (DebugHelper.DebugOutputPath)

    if (opt.isDefined) {
      val fullPath = new Path (opt.get, relativePath).toString
      fn (fullPath)
    }
  }

  def justDo (sc: SparkContext)(fn: => Unit) {

    val opt = sc.getConf.getOption (DebugHelper.DebugOutputPath)

    if (opt.isDefined) {
      fn
    }
  }

  val DebugOutputPath = "DebugOutputPath"
}
