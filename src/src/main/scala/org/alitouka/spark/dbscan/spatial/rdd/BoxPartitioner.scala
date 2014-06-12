package org.alitouka.spark.dbscan.spatial.rdd

import org.apache.spark.Partitioner
import org.alitouka.spark.dbscan.spatial.{PointSortKey, Point, Box}
import org.alitouka.spark.dbscan.BoxId

/** A partitioner which assigns each entry in a dataset to a [[org.alitouka.spark.dbscan.spatial.Box]]
  *
  * @param boxes A collection of [[org.alitouka.spark.dbscan.spatial.Box]]es
  */
private [dbscan] class BoxPartitioner (val boxes: Iterable[Box]) extends Partitioner {

  assert (boxes.forall(_.partitionId >= 0))

  private val boxIdsToPartitions = generateBoxIdsToPartitionsMap(boxes)

  override def numPartitions: Int = boxes.size

  def getPartition(key: Any): Int = {

    key match {
      case k: PointSortKey => boxIdsToPartitions(k.boxId)
      case boxId: BoxId => boxIdsToPartitions(boxId)
      case pt: Point => boxIdsToPartitions(pt.boxId)
      case _ => 0 // throw an exception?
    }
  }


  private def generateBoxIdsToPartitionsMap (boxes: Iterable[Box]): Map[BoxId, Int] = {
    boxes.map ( x => (x.boxId, x.partitionId)).toMap
  }
}

private [dbscan] object BoxPartitioner {

  def assignPartitionIdsToBoxes (boxes: Iterable[Box]): Iterable[Box] = {
    boxes.zip (0 until boxes.size).map ( x => x._1.withPartitionId(x._2) )
  }

}