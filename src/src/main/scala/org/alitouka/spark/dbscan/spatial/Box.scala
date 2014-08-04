package org.alitouka.spark.dbscan.spatial

import org.alitouka.spark.dbscan.{DbscanSettings, BoxId}
import org.alitouka.spark.dbscan.util.math.DoubleComparisonOperations._

/** Represents a box-shaped region in multi-dimensional space
  *
  * @param bounds Bounds of this box in each dimension
  * @param boxId A unique identifier of this box
  * @param partitionId Identifier of a data set partition which corresponds to this box
  */
private [dbscan] class Box (val bounds: Array[BoundsInOneDimension], val boxId: BoxId = 0, val partitionId: Int = -1, var adjacentBoxes: List[Box] = Nil)
  extends Serializable with Ordered[Box] {

  val centerPoint = calculateCenter (bounds)

  def this (b: List[BoundsInOneDimension], boxId: Int) = this (b.toArray, boxId)

  def this (b: Box) = this (b.bounds, b.boxId, b.partitionId, b.adjacentBoxes)

  def this (b: BoundsInOneDimension*) = this (b.toArray)

  def splitAlongLongestDimension (numberOfSplits: Int,
      idGenerator: BoxIdGenerator = new BoxIdGenerator(this.boxId)): Iterable[Box] = {

    val (longestDimension, idx) = findLongestDimensionAndItsIndex()

    val beforeLongest = if (idx > 0) bounds.take (idx) else Array[BoundsInOneDimension] ()
    val afterLongest = if (idx < bounds.size-1) bounds.drop(idx+1) else Array[BoundsInOneDimension] ()
    val splits = longestDimension.split(numberOfSplits)

    splits.map {
      s => {
        val newBounds = (beforeLongest :+ s) ++: afterLongest
        new Box (newBounds, idGenerator.getNextId())
      }
    }
  }

  def isPointWithin (pt: Point) = {

    assert (bounds.length == pt.coordinates.length)

    bounds.zip (pt.coordinates).forall( x => x._1.isNumberWithin(x._2) )
  }

  def isBigEnough (settings: DbscanSettings): Boolean = {

    bounds.forall( _.length >= 2*settings.epsilon )
  }

  def extendBySizeOfOtherBox (b: Box): Box = {

    assert (this.bounds.size == b.bounds.size)

    val newBounds = this.bounds.zip (b.bounds).map ( x => x._1.extend(x._2) )
    new Box (newBounds)
  }

  def withId (newId: BoxId): Box = {
    new Box (this.bounds, newId, this.partitionId, this.adjacentBoxes)
  }

  def withPartitionId (newPartitionId: Int): Box = {
    new Box (this.bounds, this.boxId, newPartitionId, this.adjacentBoxes)
  }

  override def toString (): String = {
    "Box " + bounds.mkString(", ") + "; id = " + boxId + "; partition = " + partitionId
  }

  private [dbscan] def findLongestDimensionAndItsIndex ()
    :(BoundsInOneDimension, Int) = {

    var idx: Int = 0
    var foundBound: BoundsInOneDimension = null
    var maxLen: Double = Double.MinValue

    for (i <- 0 until bounds.size) {
      val b = bounds(i)

      val len = b.length

      if (len > maxLen) {
        foundBound = b
        idx = i
        maxLen = len
      }
    }

    (foundBound, idx)
  }

  private [dbscan] def calculateCenter (b: Array[BoundsInOneDimension]): Point = {
    val centerCoordinates = b.map ( x => x.lower + (x.upper - x.lower) / 2 )
    new Point (centerCoordinates)
  }

  def addAdjacentBox (b: Box) = {
    adjacentBoxes = b :: adjacentBoxes
  }

  override def compare(that: Box): Int = {
    assert (this.bounds.size == that.bounds.size)

    centerPoint.compareTo(that.centerPoint)
  }

  def isAdjacentToBox (that: Box): Boolean = {

    assert (this.bounds.size == that.bounds.size)

    val (adjacentBounds, notAdjacentBounds) = this.bounds.zip(that.bounds).partition {
      x => {
        x._1.lower ~~ x._2.lower ||
        x._1.lower ~~ x._2.upper ||
        x._1.upper ~~ x._2.upper ||
        x._1.upper ~~ x._2.lower
      }
    }

    adjacentBounds.size >= 1 && notAdjacentBounds.forall {
      x => {
        (x._1.lower >~ x._2.lower && x._1.upper <~ x._2.upper) || (x._2.lower >~ x._1.lower && x._2.upper <~ x._1.upper)
      }
    }
  }
}

object Box {
  def apply (centerPoint: Point, size: Box): Box = {

    val newBounds = centerPoint.coordinates.map ( c => new BoundsInOneDimension(c, c, true) ).toArray
    new Box (newBounds).extendBySizeOfOtherBox(size)
  }
}