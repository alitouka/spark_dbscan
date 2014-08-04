package org.alitouka.spark.dbscan.spatial.rdd

import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.alitouka.spark.dbscan.spatial.{PointSortKey, BoxCalculator, Box, Point}
import org.alitouka.spark.dbscan.{BoxId, PairOfAdjacentBoxIds}


class PointsInAdjacentBoxesRDD (prev: RDD[(BoxId, Point)], val boxesWithAdjacentBoxes: Iterable[Box])
  extends ShuffledRDD [BoxId, Point, (BoxId, Point)] (prev, new BoxPartitioner(boxesWithAdjacentBoxes))

object PointsInAdjacentBoxesRDD {

  def apply (points: RDD[Point], boxesWithAdjacentBoxes: Iterable[Box]): PointsInAdjacentBoxesRDD = {


    val broadcastBoxes = points.sparkContext.broadcast(boxesWithAdjacentBoxes)

    val pointsKeyedByPairOfBoxes = points.mapPartitions {
      it => {

        val boxesMap = broadcastBoxes.value.map (x => (x.boxId, x :: x.adjacentBoxes)).toMap

        for (p <- it; b <- boxesMap(p.boxId); if p.boxId <= b.boxId)
          yield (b.boxId, p)
      }
    }

    new PointsInAdjacentBoxesRDD(pointsKeyedByPairOfBoxes, boxesWithAdjacentBoxes)
  }
}