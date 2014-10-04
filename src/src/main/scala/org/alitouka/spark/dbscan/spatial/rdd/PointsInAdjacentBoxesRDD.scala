package org.alitouka.spark.dbscan.spatial.rdd

import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.alitouka.spark.dbscan.spatial.{PointSortKey, BoxCalculator, Box, Point}
import org.alitouka.spark.dbscan.PairOfAdjacentBoxIds


/**
 * An RDD that stores points which are close to each other but
 * reside in different density-based partitions of the original data set
 * (these partitions are referred to as "boxes" below)
 *
 * Each partition of this RDD contains points in 2 adjacent boxes
 * Each point may appear multiple times in this RDD - as many times as many boxes are adjacent to the box in which
 * this point resides
 *
 * @param prev An RDD where each entry contains a pair of IDs of adjacent boxes and a point which resides in one of
 *             these boxes
 * @param adjacentBoxIdPairs A collection of distinct pairs of box IDs
 */
private [dbscan] class PointsInAdjacentBoxesRDD (prev: RDD[(PairOfAdjacentBoxIds, Point)], val adjacentBoxIdPairs: Array[PairOfAdjacentBoxIds])
  extends ShuffledRDD [PairOfAdjacentBoxIds, Point, Point] (prev, new AdjacentBoxesPartitioner(adjacentBoxIdPairs))

private [dbscan] object PointsInAdjacentBoxesRDD {

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