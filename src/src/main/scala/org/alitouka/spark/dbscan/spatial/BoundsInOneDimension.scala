package org.alitouka.spark.dbscan.spatial

import org.alitouka.spark.dbscan.DbscanSettings
import org.alitouka.spark.dbscan.util.math.DoubleComparisonOperations._

/** Represents lower and upper bound along each dimension. Very similar to the Range class
  * 
  * @param lower A lower bound
  * @param upper An upper bound
  * @param includeHigherBound Indicates whether the upper bound should be included
  */
private [dbscan] class BoundsInOneDimension (val lower: Double, val upper: Double,
  val includeHigherBound: Boolean = false)
  extends Serializable {

  /** Checks whether a number is within bounds
    *
    * @param n
    * @return
    */
  def isNumberWithin (n: Double) = {
    (n >~ lower) &&  ((n < upper) || (includeHigherBound && (n <~ upper)))
  }

  /** Splits this object into a number of equal parts
    *
    * @param n How many parts to produce. The actual number of parts may be less if the length of each part is
    *          less than 2 * epsilon
    * @param dbscanSettings Clustering settings (contains the epsilon parameter)
    * @return A list of [[org.alitouka.spark.dbscan.spatial.BoundsInOneDimension]]
    */
  def split (n: Int, dbscanSettings: DbscanSettings): List[BoundsInOneDimension] = {
    split (n, dbscanSettings.epsilon*2)
  }

  /** Splits this object into n or less equal parts, each of which is not shorter than minLen
    *
    * @param n
    * @param minLen
    * @return
    */
  def split (n: Int, minLen: Double): List[BoundsInOneDimension] = {

    val maxN = ((this.length / minLen) + 0.5).toInt

    split (Math.min (n, maxN))
  }

  /** Splits this object into n equal parts
    *
    * @param n
    * @return
    */
  def split (n: Int): List [BoundsInOneDimension] = {
    var result: List[BoundsInOneDimension] = Nil
    val increment = (upper - lower) / n
    var currentLowerBound = lower

    for (i <- 1 to n) {
      val include = if (i < n) false else this.includeHigherBound
      val newUpperBound = currentLowerBound + increment
      val newSplit = new BoundsInOneDimension(currentLowerBound, newUpperBound, include)
      result = newSplit :: result
      currentLowerBound = newUpperBound
    }

    result.reverse
  }

  def length: Double = upper - lower

  def extend (byLength: Double): BoundsInOneDimension = {
    val halfLength = byLength / 2

    new BoundsInOneDimension (this.lower - halfLength, this.upper + halfLength, this.includeHigherBound)
  }

  def extend (by: BoundsInOneDimension): BoundsInOneDimension = {
    extend (by.length)
  }

  def increaseToFit (that: BoundsInOneDimension): BoundsInOneDimension = {
    new BoundsInOneDimension (Math.min (this.lower, that.lower), Math.max (this.upper, that.upper),
      this.includeHigherBound || that.includeHigherBound)
  }

  override def toString (): String = {
    "[" + lower + " - " + upper + (if (includeHigherBound) "]" else ")")
  }

  override def equals (that: Any): Boolean = {
    if (that.isInstanceOf[BoundsInOneDimension]) {
      val typedThat = that.asInstanceOf[BoundsInOneDimension]

      typedThat.canEqual(this) &&
        this.lower == typedThat.lower &&
        this.upper == typedThat.upper &&
        this.includeHigherBound == typedThat.includeHigherBound
    }
    else {
      false
    }
  }

  override def hashCode (): Int = {
    41 * (41 * (41 + (if (includeHigherBound) 1 else 0)) + lower.toInt) + upper.toInt
  }

  def canEqual(other: Any) = other.isInstanceOf[BoundsInOneDimension]
}

private [dbscan] object BoundsInOneDimension {
  implicit def tupleOfDoublesToBounds (x: (Double, Double)): BoundsInOneDimension = {
    new BoundsInOneDimension (x._1, x._2)
  }

  implicit def tupleOfDoublesAndBoolToBounds (x: (Double, Double, Boolean)): BoundsInOneDimension = {
    new BoundsInOneDimension(x._1, x._2, x._3)
  }
}