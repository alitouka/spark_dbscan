package org.alitouka.spark.dbscan

import org.scalatest.{FunSuite, BeforeAndAfterEach, Matchers, FlatSpec}
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.alitouka.spark.dbscan.spatial.{PointSortKey, Point}

class SuiteBase extends FunSuite with Matchers with BeforeAndAfterEach with Logging {

  val sc = TestContextHolder.sc

    protected def readDataset (path: String) = {

      val rawData = sc.textFile (path)

      rawData.map (
        line => {
          val split = line.split(",")
          new Point (Array (split(0).toDouble, split(1).toDouble))
        }
      )

    }

  def createRDDOfPoints (sc: SparkContext,
                         points: (Double, Double)*) = {

    val pointIds = 1 to points.size

    val pointObjects = points
      .zip (pointIds)
      .map ( x => create2DPoint(x._1._1, x._1._2).withPointId(x._2) )

    sc.parallelize(pointObjects)
  }

  def create2DPoint (x: Double, y: Double, idx: PointId = 0): Point = {
    new Point ( new PointCoordinates (Array (x, y)), idx, 1, Math.sqrt (x*x+y*y))
  }

  def create2DPointWithSortKey (x: Double, y: Double, idx: PointId = 0): (PointSortKey, Point) = {

    val pt = create2DPoint (x, y, idx)
    val sortKey = new PointSortKey (pt)

    (sortKey, pt)
  }
}
