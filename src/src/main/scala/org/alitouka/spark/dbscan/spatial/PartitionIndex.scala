package org.alitouka.spark.dbscan.spatial

import org.alitouka.spark.dbscan.spatial.rdd.PartitioningSettings
import org.alitouka.spark.dbscan.DbscanSettings
import scala.collection.mutable.{ListBuffer, ArrayBuffer}
import org.alitouka.spark.dbscan.util.math.DoubleComparisonOperations._
import scala.collection.parallel.ParIterable
import org.alitouka.spark.dbscan.util.debug.Clock

/** An indexing data structure which allows fast lookup of point's neighbors
  *
  * Organizes points into a multi-level hierarchy of box-shaped regions. When asked to find point's neighbors,
  * searches for them in the region which contains the specified point and, if necessary,
  * in regions adjacent to it
  *
  * @param partitionBounds A box which embraces all points to be indexed
  * @param dbscanSettings Clustering settings
  * @param partitioningSettings Partitioning settings
  */
private [dbscan] class PartitionIndex (val partitionBounds: Box,
  val dbscanSettings: DbscanSettings,
  val partitioningSettings: PartitioningSettings) extends DistanceCalculation with Serializable {

  implicit val distanceMeasure = dbscanSettings.distanceMeasure

  private [dbscan] val boxesTree = PartitionIndex.buildTree (partitionBounds, partitioningSettings, dbscanSettings)
  private val largeBox = PartitionIndex.createBoxTwiceLargerThanLeaf(boxesTree)


  /** Populates the index with points
    *
    * @param points
    * @tparam T
    */
  def populate [T <: Iterable[Point]] (points: T): Unit = {
    populate (points.iterator)
  }

  def populate (points: Array[Point]): Unit = {
    populate (points.iterator)
  }


  def populate (points: Iterator[Point]): Unit = {
    val clock = new Clock ()

    points.foreach {
      pt => {
        findBoxAndAddPoint(pt, boxesTree)
      }
    }

    clock.logTimeSinceStart("Population of partition index")
  }


  /** Finds point's neighbors
    *
    * @param pt A point whose neighbors shuold be found
    * @return A collection of pt's neighbors
    */
  def findClosePoints (pt: Point): Iterable[Point] = {
    findPotentiallyClosePoints(pt).filter ( p => p.pointId != pt.pointId && calculateDistance (p, pt) <= dbscanSettings.epsilon )
  }


  private [dbscan] def findPotentiallyClosePoints (pt: Point): Iterable[Point] = {
    val box1 = findBoxForPoint(pt, boxesTree)
    var result = ListBuffer[Point] ()

    result ++= box1.points.filter ( p => p.pointId != pt.pointId && Math.abs(p.distanceFromOrigin - pt.distanceFromOrigin) <= dbscanSettings.epsilon )

    if (this.isPointCloseToAnyBound(pt, box1.box, dbscanSettings.epsilon)) {

      box1.adjacentBoxes.foreach {
        box2 => {
          val tempBox = Box (pt, largeBox)

          if (tempBox.isPointWithin(box2.box.centerPoint)) {
            result ++= box2.points.filter ( p => Math.abs(p.distanceFromOrigin - pt.distanceFromOrigin) <= dbscanSettings.epsilon )
          }
        }
      }
    }

    result
  }


  private def findBoxAndAddPoint (pt: Point, root: BoxTreeItemWithPoints): Unit = {
    val b = findBoxForPoint(pt, root)
    b.points += pt
  }


  private [dbscan] def findBoxForPoint (pt: Point, root: BoxTreeItemWithPoints): BoxTreeItemWithPoints = {
    if (root.children.isEmpty) {
      root
    }
    else {
     val child = root.children.find ( x => x.box.isPointWithin(pt) )

      child match {
        case b: Some[BoxTreeItemWithPoints] => findBoxForPoint (pt, b.get)
        case _ => {
          throw new Exception (s"Box for point $pt was not found")
        }
      }
    }
  }
}


private [dbscan] object PartitionIndex extends DistanceCalculation {


  def buildTree (boundingBox: Box,
                 partitioningSettings: PartitioningSettings,
                 dbscanSettings: DbscanSettings): BoxTreeItemWithPoints = {

    val sortedBoxes = generateAndSortBoxes(boundingBox,
      partitioningSettings.numberOfSplitsWithinPartition, dbscanSettings)

    buildTree (boundingBox, sortedBoxes)
  }


  def buildTree (boundingBox: Box, sortedBoxes: Array[Box]): BoxTreeItemWithPoints = {

    val leafs = sortedBoxes.map (b =>  new BoxTreeItemWithPoints (b))

    leafs.foreach {
      leaf => leaf.adjacentBoxes ++= findAdjacentBoxes(leaf, leafs)
    }

    val root = new BoxTreeItemWithPoints(boundingBox)
    root.children = generateSubitems (root, 0, leafs, 0, leafs.size-1).toList

    root
  }


  def generateSubitems (root: BoxTreeItemWithPoints, dimension: Int, leafs: Array[BoxTreeItemWithPoints], start: Int,
    end: Int): Iterable[BoxTreeItemWithPoints] = {

    val numberOfDimensions = root.box.bounds.size
    var result: List[BoxTreeItemWithPoints] = Nil

    if (dimension < numberOfDimensions) {
      var nodeStart = start
      var nodeEnd = start

      while (nodeStart <= end) {

        val b = leafs(nodeStart).box.bounds(dimension)
        val leafsSubset =  ArrayBuffer[BoxTreeItemWithPoints] ()

        while (nodeEnd <= end && leafs(nodeEnd).box.bounds(dimension) == b) {
          leafsSubset += leafs(nodeEnd)
          nodeEnd += 1
        }

        var newSubitem: BoxTreeItemWithPoints = null

        if (leafsSubset.size > 1) {
          val embracingBox = PartitionIndex.generateEmbracingBox(leafsSubset, numberOfDimensions)
          newSubitem = new BoxTreeItemWithPoints(embracingBox)
          newSubitem.children = generateSubitems (newSubitem, dimension+1, leafs, nodeStart, nodeEnd-1).toList
        }
        else if (leafsSubset.size == 1) {
          newSubitem = leafsSubset(0)
        }

        result = newSubitem :: result
        nodeStart = nodeEnd
      }
    }

    result.reverse
  }


  def generateEmbracingBox (subitems: Iterable[BoxTreeItemWithPoints], numberOfDimensions: Int): Box = {

    var dimensions: ArrayBuffer[BoundsInOneDimension] = ArrayBuffer[BoundsInOneDimension] ()

    (0 until numberOfDimensions).foreach {
      i => {
        val zeroValue = new BoundsInOneDimension(Double.MaxValue, Double.MinValue, false)
        val x = subitems.map ( _.box.bounds(i) )
        val newDimension = x.fold(zeroValue) {
          (a,b) =>
            new BoundsInOneDimension (
              Math.min (a.lower, b.lower),
              Math.max (a.upper, b.upper),
              a.includeHigherBound || b.includeHigherBound
            )
        }

        dimensions += newDimension
      }
    }

    new Box (dimensions.toArray)
  }


  def generateAndSortBoxes (boundingBox: Box, maxNumberOfSplits: Int, dbscanSettings: DbscanSettings): Array [Box] = {
    BoxCalculator
      .splitBoxIntoEqualBoxes(boundingBox, maxNumberOfSplits, dbscanSettings)
      .toArray
      .sortWith(_<_)
  }


  def findAdjacentBoxes (x: BoxTreeItemWithPoints, boxes: Iterable[BoxTreeItemWithPoints])
    :Iterable[BoxTreeItemWithPoints] = {

    var result: List[BoxTreeItemWithPoints] = Nil

    boxes.filter ( _ != x).foreach {
      y => {

        // The code below relies on the fact that all boxes are of equal size
        // It works a little faster than Box.isAdjacentToBox

        var n = 0

        for (i <- 0 until x.box.bounds.size) {
          val cx = x.box.centerPoint.coordinates(i)
          val cy = y.box.centerPoint.coordinates(i)
          val d = Math.abs (cx - cy)

          if ((d ~~ 0) || (d ~~ x.box.bounds(i).length)) {
            n += 1
          }
        }

        if (n == x.box.bounds.size) {
          result = y :: result
        }
      }
    }

    result
  }


  def createBoxTwiceLargerThanLeaf (root: BoxTreeItemWithPoints): Box = {
    val leaf = findFirstLeafBox(root)
    leaf.extendBySizeOfOtherBox(leaf)
  }


  def findFirstLeafBox (root: BoxTreeItemWithPoints): Box = {
    var result = root

    while (!result.children.isEmpty) {
      result = result.children(0)
    }

    result.box
  }
}
