package org.alitouka.spark.dbscan.spatial.rdd

import org.apache.spark.Partitioner
import org.alitouka.spark.dbscan.spatial.{BoxCalculator, Box}
import org.alitouka.spark.dbscan.{PairOfAdjacentBoxIds, BoxId}


class AdjacentBoxesPartitioner (private val adjacentBoxIdPairs: Array[PairOfAdjacentBoxIds]) extends Partitioner {

  def this (boxesWithAdjacentBoxes: Iterable[Box]) = this (BoxCalculator.generateDistinctPairsOfAdjacentBoxIds(boxesWithAdjacentBoxes).toArray)

  override def numPartitions: Int = adjacentBoxIdPairs.length

  override def getPartition(key: Any): Int = {
    key match {
      case (b1: BoxId, b2: BoxId) => adjacentBoxIdPairs.indexOf((b1, b2))
      case _ => 0 // Throw an exception?
    }
  }
}
