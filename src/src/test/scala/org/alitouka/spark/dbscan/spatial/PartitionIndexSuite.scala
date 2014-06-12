package org.alitouka.spark.dbscan.spatial

import org.alitouka.spark.dbscan.{DbscanSettings, RawDataSet, SuiteBase, TestDatasets}
import org.apache.spark.rdd.RDD
import org.alitouka.spark.dbscan.spatial.rdd.PartitioningSettings
import org.apache.commons.math3.ml.distance.DistanceMeasure

class PartitionIndexSuite extends SuiteBase with TestDatasets with DistanceCalculation  {

  class TestCase (val data: RDD[Point], val epsilon: Double)

  test ("PartitionIndex should correctly identify close points in all test datasets") {

    val allCases = Array (
      new TestCase (dataset0, 2.0),
      new TestCase (dataset0_1, 2.0),
      new TestCase (dataset1, 0.8),
      new TestCase (dataset1_1, 0.8),
      new TestCase (dataset2, 0.8),
      new TestCase (dataset4, 1))

    allCases.foreach ( runTestCase )
  }

  test("PartitionIndex should build 3-level tree") {

    val boundingBox = new Box ((0.0, 3.0), (0.0, 2.0), (0.0, 2.0))
    val dbscanSettings = new DbscanSettings ().withEpsilon (0.5)

    val root = PartitionIndex.buildTree (boundingBox, new PartitioningSettings (), dbscanSettings)

    root.children.size should be (3)

    root.children.foreach {
      child => {
        child.children.size should be (2)

        child.children.foreach {
          grandchild => {
            grandchild.children.size should be (2)

            grandchild.children.foreach {
              ggchild => {
                ggchild.children.size should be (0)
              }
            }
          }
        }
      }
    }
  }

  test("PartitionIndex should put points into correct boxes") {
    val boundingBox = new Box ((0.0, 3.0), (0.0, 2.0), (0.0, 2.0))
    val dbscanSettings = new DbscanSettings ().withEpsilon (0.5)

    val points = Array (new Point (0.2, 0.2, 0.2), new Point (0.5, 0.5, 0.5),
      new Point (2.2, 0.2, 0.2), new Point (2.2, 0.5, 0.5),
      new Point (2.5, 1.5, 1.5))

    val idx = new PartitionIndex (boundingBox, dbscanSettings, new PartitioningSettings ())
    idx.populate (points)

    val b1 = idx.findBoxForPoint (points(0), idx.boxesTree)
    b1.points.size should be (2)
    b1.points should contain (points(1))

    val b2 = idx.findBoxForPoint (points(2), idx.boxesTree)
    b2.points.size should be (2)
    b2.points should contain (points (3))

    val b3 = idx.findBoxForPoint(points(4), idx.boxesTree)
    b3.points.size should be (1)
  }

  test("PartitionIndex should find adjacent boxes properly") {

    val dbscanSettings = new DbscanSettings ().withEpsilon (0.5)

    val boundingBox2D = new Box ((0.0, 4.0), (0.0, 4.0))
    val points2D = Array (new Point (0.5, 0.5), new Point (1.5, 0.5), new Point (1.5, 1.5), new Point (3.5, 0.5),
      new Point (1.5, 3.5), new Point (3.5, 3.5))

    val idx2D = new PartitionIndex (boundingBox2D, dbscanSettings, new PartitioningSettings ())
    idx2D.populate(points2D)

    checkNumberOfAdjacentBoxes(idx2D, points2D(0), 3)
    checkNumberOfAdjacentBoxes(idx2D, points2D(1), 5)
    checkNumberOfAdjacentBoxes(idx2D, points2D(2), 8)
    checkNumberOfAdjacentBoxes(idx2D, points2D(3), 3)
    checkNumberOfAdjacentBoxes(idx2D, points2D(4), 5)
    checkNumberOfAdjacentBoxes(idx2D, points2D(5), 3)

    val boundingBox3D = new Box ((0.0, 4.0), (0.0, 4.0), (0.0, 4.0))
    val points3D = Array (new Point (0.5, 0.5, 0.5), new Point (1.5, 0.5, 0.5), new Point (1.5, 1.5, 1.5), new Point (3.5, 0.5, 0.5),
      new Point (1.5, 3.5, 0.5), new Point (3.5, 3.5, 3.5))

    val idx3D = new PartitionIndex (boundingBox3D, dbscanSettings, new PartitioningSettings ())
    idx3D.populate(points3D)

    checkNumberOfAdjacentBoxes(idx3D, points3D(0), 7)
    checkNumberOfAdjacentBoxes(idx3D, points3D(1), 11)
    checkNumberOfAdjacentBoxes(idx3D, points3D(2), 26)
    checkNumberOfAdjacentBoxes(idx3D, points3D(3), 7)
    checkNumberOfAdjacentBoxes(idx3D, points3D(4), 11)
    checkNumberOfAdjacentBoxes(idx3D, points3D(5), 7)

  }

  private def checkNumberOfAdjacentBoxes (idx: PartitionIndex, pt: Point, expectedCount: Int): Unit = {
    val box = idx.findBoxForPoint(pt, idx.boxesTree)
    box.adjacentBoxes.size should be (expectedCount)
  }




  private def runTestCase (tc: TestCase): Unit = {

    val boxCalculator = new BoxCalculator(tc.data)
    val boundingBox = boxCalculator.calculateBoundingBox
    val allPoints = tc.data.collect ()
    val dbscanSettings = new DbscanSettings ().withEpsilon (tc.epsilon)

    val partitionIndex = new PartitionIndex (boundingBox, dbscanSettings, new PartitioningSettings ())

    partitionIndex.populate(allPoints)

    allPoints.foreach ( p => testPoint (p, allPoints, partitionIndex, dbscanSettings))
  }

  private def testPoint (pt: Point, allPoints: Iterable[Point], partitionIndex: PartitionIndex, dbscanSettings: DbscanSettings): Unit = {

    implicit val distanceMeasure = dbscanSettings.distanceMeasure

    val etalonClosePoints = allPoints.filter ( p => p != pt && calculateDistance (p, pt) <= dbscanSettings.epsilon ).toSet
    val foundClosePoints = partitionIndex.findClosePoints(pt).toSet

//    if (etalonClosePoints != foundClosePoints) {
//      fail ("")
//    }

    etalonClosePoints should equal (foundClosePoints)
  }


}
