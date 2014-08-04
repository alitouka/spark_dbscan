package org.alitouka.spark.dbscan.spatial.rdd

import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.alitouka.spark.dbscan.spatial.{PointSortKey, BoxCalculator, Box, Point}
import org.alitouka.spark.dbscan.PairOfAdjacentBoxIds


class PointsInAdjacentBoxesRDD (prev: RDD[(PairOfAdjacentBoxIds, Point)], val adjacentBoxIdPairs: Array[PairOfAdjacentBoxIds])
  extends ShuffledRDD [PairOfAdjacentBoxIds, Point, (PairOfAdjacentBoxIds, Point)] (prev, new AdjacentBoxesPartitioner(adjacentBoxIdPairs))

object PointsInAdjacentBoxesRDD {

  def apply (points: RDD[Point], boxesWithAdjacentBoxes: Iterable[Box]): PointsInAdjacentBoxesRDD = {
    val adjacentBoxIdPairs = BoxCalculator.generateDistinctPairsOfAdjacentBoxIds(boxesWithAdjacentBoxes).toArray

    val broadcastBoxIdPairs = points.sparkContext.broadcast(adjacentBoxIdPairs)

    val pointsKeyedByPairOfBoxes = points.mapPartitions {
      it => {

        val boxIdPairs = broadcastBoxIdPairs.value

        for (p <- it; pair <- boxIdPairs; if p.boxId == pair._1 || p.boxId == pair._2)
          yield (pair, p)
      }
    }

    new PointsInAdjacentBoxesRDD(pointsKeyedByPairOfBoxes, adjacentBoxIdPairs)
  }
}
