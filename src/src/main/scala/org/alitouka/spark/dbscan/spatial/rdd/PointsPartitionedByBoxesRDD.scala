package org.alitouka.spark.dbscan.spatial.rdd

import org.alitouka.spark.dbscan.spatial.{BoxCalculator, PointSortKey, Box, Point}
import org.apache.spark.rdd.{ShuffledRDD, RDD}
import org.alitouka.spark.dbscan._
import org.apache.spark.SparkContext
import org.alitouka.spark.dbscan.util.PointIndexer

/** Density-based partitioned RDD where each point is accompanied by its sort key
  *
  * @param prev
  * @param boxes
  * @param boundingBox
  */
private [dbscan] class PointsPartitionedByBoxesRDD  (prev: RDD[(PointSortKey, Point)], val boxes: Iterable[Box], val boundingBox: Box)
  extends ShuffledRDD [PointSortKey, Point, Point] (prev, new BoxPartitioner(boxes))

object PointsPartitionedByBoxesRDD {

  def apply (rawData: RawDataSet,
    partitioningSettings: PartitioningSettings = new PartitioningSettings (),
    dbscanSettings: DbscanSettings = new DbscanSettings ())
    : PointsPartitionedByBoxesRDD = {

    val sc = rawData.sparkContext
    val boxCalculator = new BoxCalculator (rawData)
    val (boxes, boundingBox) = boxCalculator.generateDensityBasedBoxes (partitioningSettings, dbscanSettings)
    val broadcastBoxes = sc.broadcast(boxes)
    var broadcastNumberOfDimensions = sc.broadcast (boxCalculator.numberOfDimensions)

    val pointsInBoxes = PointIndexer.addMetadataToPoints(
      rawData,
      broadcastBoxes,
      broadcastNumberOfDimensions,
      DbscanSettings.getDefaultDistanceMeasure)

    PointsPartitionedByBoxesRDD (pointsInBoxes, boxes, boundingBox)
  }

  def apply (pointsInBoxes: RDD[(PointSortKey, Point)], boxes: Iterable[Box], boundingBox: Box): PointsPartitionedByBoxesRDD = {
    new PointsPartitionedByBoxesRDD(pointsInBoxes, boxes, boundingBox)
  }


  private [dbscan] def extractPointIdsAndCoordinates (data: RDD[(PointSortKey, Point)]): RDD[(PointId, PointCoordinates)] = {
    data.map ( x => (x._2.pointId, x._2.coordinates) )
  }

}


