package org.alitouka.spark.dbscan.util.debug

import org.apache.spark.Logging

/**
 *
 */
private [dbscan] trait Troubleshooting extends Logging {

  def logEntry(): Unit = {
    logMessageFollowedByCallingMethodName("Entering")
  }

  def logExit(): Unit = {
    logMessageFollowedByCallingMethodName("Exiting")
  }

  private def logMessageFollowedByCallingMethodName (message: String): Unit = {

    if (this.log.isDebugEnabled) {
      val stackTrace = Thread.currentThread().getStackTrace
      var methodName: String = "[undefined method]"

      if (stackTrace.length > 4) {
        methodName = stackTrace(4).getMethodName
      }

      logDebug(message + " " + methodName)
    }
  }

}
