package org.alitouka.spark.dbscan.spatial

import org.alitouka.spark.dbscan._

private [dbscan] class BoxIdGenerator (val initialId: BoxId) {
  var nextId = initialId

  def getNextId (): BoxId = {
    nextId += 1
    nextId
  }
}
