package org.alitouka.spark.dbscan.spatial

import org.apache.spark.rdd.{ShuffledRDD, RDD}
import org.apache.spark.SparkContext._
import org.alitouka.spark.dbscan.spatial.rdd._
import org.apache.commons.math3.ml.distance.DistanceMeasure
import org.alitouka.spark.dbscan._
import scala.collection.mutable.ListBuffer
import org.alitouka.spark.dbscan.util.debug.DebugHelper
import org.apache.spark.internal.Logging
import scala.collection.mutable


private [dbscan] class DistanceAnalyzer (
  private val settings: DbscanSettings,
  private val partitioningSettings: PartitioningSettings = new PartitioningSettings ())
  extends Serializable
  with DistanceCalculation
  with Logging {


  implicit val distanceMeasure: DistanceMeasure = settings.distanceMeasure

  def this () = this (new DbscanSettings ())

  def countNeighborsForEachPoint ( data: PointsPartitionedByBoxesRDD )
      : RDD[(PointSortKey, Point)] = {

    val closePointCounts: RDD[(PointSortKey, Long)] = countClosePoints (data)
      .foldByKey(1)(_+_)
      .cache ()

    val pointsWithoutNeighbors = data
      .keys
      .subtract(closePointCounts.keys)
      .map ( x => (x, 1L))

    val allPointCounts = closePointCounts.union (pointsWithoutNeighbors)
    val partitionedAndSortedCounts = new ShuffledRDD [PointSortKey, Long, Long] (
      allPointCounts,
      new BoxPartitioner (data.boxes)
    ).mapPartitions (sortPartition, true)

    val sortedData = data.mapPartitions (sortPartition, true)

    val pointsWithCounts = sortedData.zipPartitions(partitionedAndSortedCounts, true) {
      (it1, it2) => {
        it1.zip (it2).map {
          x => {
            assert (x._1._1.pointId == x._2._1.pointId)

            val newPt = new Point (x._1._2).withNumberOfNeighbors(x._2._2)
            (new PointSortKey (newPt), newPt)
          }
        }
      }
    }

    DebugHelper.doAndSaveResult(data.sparkContext, "restoredPartitions") {
      path => {

        pointsWithCounts.mapPartitionsWithIndex {
          (idx, it) => {
            it.map ( x => x._2.coordinates.mkString (",") + "," + idx)
          }
        }.saveAsTextFile(path)
      }
    }

    pointsWithCounts
  }

  private def sortPartition [T] (it: Iterator[(PointSortKey, T)]) : Iterator[(PointSortKey, T)] = {

    val ordering = implicitly[Ordering[PointSortKey]]
    val buf = it.toArray
    buf.sortWith((x, y) => ordering.lt(x._1, y._1)).iterator
  }

  def countClosePoints ( data: PointsPartitionedByBoxesRDD)
    :RDD[(PointSortKey, Long)] = {

    val closePointsInsideBoxes = countClosePointsWithinEachBox(data)
    val pointsCloseToBoxBounds = findPointsCloseToBoxBounds (data, data.boxes, settings.epsilon)

    val closePointsInDifferentBoxes = countClosePointsInDifferentBoxes (pointsCloseToBoxBounds, data.boxes,
      settings.epsilon)

    closePointsInsideBoxes.union (closePointsInDifferentBoxes)
  }

  def countClosePointsWithinEachBox (data: PointsPartitionedByBoxesRDD)
    :RDD[(PointSortKey, Long)] = {

    val broadcastBoxes = data.sparkContext.broadcast(data.boxes)

    data.mapPartitionsWithIndex {
      (partitionIndex, it) => {

        val boxes = broadcastBoxes.value
        val boundingBox = boxes.find ( _.partitionId == partitionIndex ).get

        countClosePointsWithinPartition (it, boundingBox)
      }
    }
  }

  def countClosePointsWithinPartition (it: Iterator[(PointSortKey, Point)], boundingBox: Box): Iterator[(PointSortKey, Long)] = {

    val (it1, it2) = it.duplicate
    val partitionIndex = new PartitionIndex (boundingBox, settings, partitioningSettings)
    val counts = mutable.HashMap [PointSortKey, Long] ()
    //val boxIds = mutable.HashSet [BoxId] ()

    partitionIndex.populate(it1.map ( _._2 ))


    it2.foreach {
      currentPoint => {

        val closePointsCount: Long = partitionIndex
          .findClosePoints(currentPoint._2)
          .size

        addPointCount(counts, currentPoint._1, closePointsCount)
      }
    }

    counts.iterator
  }

  def findPointsCloseToBoxBounds [U <: RDD[(PointSortKey, Point)]] (data: U, boxes: Iterable[Box], eps: Double)
    :RDD[Point] = {

    val broadcastBoxes = data.sparkContext.broadcast(boxes)

    data.mapPartitionsWithIndex( (index, it) => {

      val box = broadcastBoxes.value.find ( _.partitionId == index ).get

      it.filter ( x => isPointCloseToAnyBound (x._2, box, eps)).map ( _._2 )
    })
  }

  def countClosePointsInDifferentBoxes (data: RDD[Point], boxesWithAdjacentBoxes: Iterable[Box], eps: Double): RDD[(PointSortKey, Long)] = {

    val pointsInAdjacentBoxes: RDD[(PairOfAdjacentBoxIds, Point)] = PointsInAdjacentBoxesRDD (data, boxesWithAdjacentBoxes)


    pointsInAdjacentBoxes.mapPartitionsWithIndex {
      (idx, it) => {

        val pointsInPartition = it.map (_._2).toArray.sortBy(_.distanceFromOrigin)
        val counts = mutable.HashMap [PointSortKey, Long] ()

        for (i <- 1 until pointsInPartition.length) {
          var j = i-1

          val pi = pointsInPartition(i)

          val piSortKey = new PointSortKey (pi)

          //boxIds += pi.boxId
          //boxIds += pj.boxId

          while (j >= 0 && pi.distanceFromOrigin - pointsInPartition(j).distanceFromOrigin <= eps) {

            val pj = pointsInPartition(j)

            if (pi.boxId != pj.boxId && calculateDistance(pi, pj) <= settings.epsilon) {
              addPointCount(counts,piSortKey, 1)
              addPointCount (counts, new PointSortKey (pj), 1)
            }

            j -= 1
          }
        }

        counts.iterator
      }
    }
  }

  private [dbscan] def addPointCount (counts: mutable.HashMap[PointSortKey, Long], sortKey: PointSortKey, c: Long) = {

    if (counts.contains(sortKey)) {
      val newCount = counts.get(sortKey).get + c
      counts.put (sortKey, newCount)
    }
    else {
      counts.put (sortKey, c)
    }
  }

  private def adjustPartitioningSettingsForSearchingClosePointsInDifferentBoxes (baseSettings: PartitioningSettings) = {

    // TODO: implement something smarter :)

    // The idea is to split the data set into regions whose edges do not coincide with edges of density-based partitions
    // A simple (but stupid) way to do that is to split a data set into N parts along each dimension, where N is a prime number

    baseSettings.withNumberOfSplitsWithinPartition(13)
  }

  def findNeighborsOfNewPoint (clusteredAndNoisePoints: RDD[Point], newPoint: PointCoordinates): RDD[Point] = {
    clusteredAndNoisePoints.filter( pt => calculateDistance(pt.coordinates, newPoint) <= settings.epsilon )
  }
}
