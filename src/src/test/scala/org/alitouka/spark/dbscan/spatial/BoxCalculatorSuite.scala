package org.alitouka.spark.dbscan.spatial

import org.alitouka.spark.dbscan.spatial.BoundsInOneDimension._
import org.alitouka.spark.dbscan.{SuiteBase, DbscanSettings}
import org.alitouka.spark.dbscan.spatial.rdd.PartitioningSettings

class BoxCalculatorSuite extends SuiteBase {



  val dataset1 = sc.parallelize(Array (
    new Point (2.5, 0), new Point (0, 1.5), new Point (4, 0.5), new Point (2.5, 2.0), // Border points
    new Point (0.2, 0.2), new Point (0.2, 0.7), new Point (0.7, 0.2),                 // Close points within one box
    new Point (2.8, 0.8), new Point (3.3, 0.8), new Point (2.8, 1.3)))                // Close points in different boxes



  test ("BoxCalculator should generate 2-level hierarchy of boxes") {

    val dbscanSettings = new DbscanSettings ().withEpsilon(1)
    val partitioningSettings = new PartitioningSettings(2, 8) // Despite 8-level settings,
                                                              // only 2 levels should be generated
                                                              // because boxes will become too small
                                                              // at deeper levels

    val rootBox = new Box ((0.0, 6.0), (0.0, 2.0))

    val boxTreeRoot = BoxCalculator.generateTreeOfBoxes(rootBox, partitioningSettings, dbscanSettings)

    val leaf1 = boxTreeRoot.children(0)
    val leaf2 = boxTreeRoot.children(1)

    leaf1.children.size should be (0)
    leaf2.children.size should be (0)

    leaf1.box.bounds(0).lower should be (0)
    leaf1.box.bounds(0).upper should be (3)
    leaf1.box.bounds(1).lower should be (0)
    leaf1.box.bounds(1).upper should be (2)

    leaf2.box.bounds(0).lower should be (3)
    leaf2.box.bounds(0).upper should be (6)
    leaf2.box.bounds(1).lower should be (0)
    leaf2.box.bounds(1).upper should be (2)

    val flatList = boxTreeRoot.flattenBoxes
    flatList.size should be (3)
  }

  test ("BoxCalculator should generate 3-level hierarchy of boxes") {
    val dbscanSettings = new DbscanSettings ().withEpsilon(1)
    val partitioningSettings = new PartitioningSettings(2, 8)

    val rootBox = new Box ((0.0, 8.0), (0.0, 2.0))

    val boxTreeRoot = BoxCalculator.generateTreeOfBoxes(rootBox, partitioningSettings, dbscanSettings)

    val node1 = boxTreeRoot.children(0)
    val node2 = boxTreeRoot.children(1)

    node1.children.size should be (2)
    node2.children.size should be (2)

    val leaf1 = node1.children(0)
    val leaf2 = node1.children(1)
    val leaf3 = node2.children(0)
    val leaf4 = node2.children(1)

    leaf1.children.size should be (0)
    leaf2.children.size should be (0)
    leaf3.children.size should be (0)
    leaf4.children.size should be (0)

    val flatList = boxTreeRoot.flattenBoxes
    flatList.size should be (7)
  }

  test("BoxCalculator should put 5 points into 3 boxes") {
    val dbscanSettings = new DbscanSettings ().withEpsilon(1)
    val partitioningSettings = new PartitioningSettings(2, 8)

    val rootBox = new Box ((0.0, 8.0), (0.0, 2.0))

    val boxTreeRoot = BoxCalculator.generateTreeOfBoxes(rootBox, partitioningSettings, dbscanSettings)

    val points = Array (new Point (1.0, 1.5), new Point (3.0, 0.5), new Point (4.5, 0.5),
      new Point (5.5, 0.5), new Point (6.5, 1.5))

    points.foreach ( pt => BoxCalculator.countOnePoint(pt, boxTreeRoot))

    val node1 = boxTreeRoot.children(0)
    val node2 = boxTreeRoot.children(1)
    val leaf1 = node1.children(0)
    val leaf2 = node1.children(1)
    val leaf3 = node2.children(0)
    val leaf4 = node2.children(1)

    node1.numberOfPoints should be (2)
    node2.numberOfPoints should be (3)

    leaf1.numberOfPoints should be (1)
    leaf2.numberOfPoints should be (1)
    leaf3.numberOfPoints should be (2)
    leaf4.numberOfPoints should be (1)

    val allBoxes = boxTreeRoot.flattenBoxes( x => x.numberOfPoints >= 2 ).toArray
    allBoxes.size should be (3)

    val boxesWithPoints = points.map ( pt => allBoxes.find ( _.isPointWithin(pt))).map ( _.get).toArray
    boxesWithPoints.size should equal (points.size)

    boxesWithPoints(0) should equal (boxesWithPoints (1))
    boxesWithPoints(2) should equal (boxesWithPoints (3))
    boxesWithPoints(4) should not equal (boxesWithPoints(1))
    boxesWithPoints(4) should not equal (boxesWithPoints(2))
  }

  test("BoxCalculator should count points in each partition") {
    val dbscanSettings = new DbscanSettings ().withEpsilon(1)
    val partitioningSettings = new PartitioningSettings(2, 8)

    val rootBox = new Box ((0.0, 8.0), (0.0, 2.0))

    val root1 = BoxCalculator.generateTreeOfBoxes(rootBox, partitioningSettings, dbscanSettings)
    val root2 = BoxCalculator.generateTreeOfBoxes(rootBox, partitioningSettings, dbscanSettings)

    val partition1 = Array (new Point (1.0, 1.5), new Point (5.5, 0.5))
    val partition2 = Array (new Point (3.0, 0.5), new Point (4.5, 0.5), new Point (6.5, 1.5))

    val counts1 = BoxCalculator.countPointsInOnePartition(root1, partition1.iterator).toArray
    val counts2 = BoxCalculator.countPointsInOnePartition(root2, partition2.iterator).toArray

    val summedCounts = (counts1 ++ counts2).groupBy( _._1 ).map ( x => (x._1, x._2.map(_._2))).map ( x => (x._1, x._2.sum)).toMap

    val boxesWithEnoughPoints = root1.flattenBoxes( x => summedCounts(x.box.boxId) >= 2)

    boxesWithEnoughPoints.size should be (3)

    // TODO: add more assertions
  }


  test("BoxCalculator should correctly identify the number of dimensions") {
    val boxCalculator = new BoxCalculator (dataset1)

    val numberOfDimensions = boxCalculator.numberOfDimensions
    numberOfDimensions should be (2)

    val bounds = boxCalculator.calculateBounds(dataset1, numberOfDimensions)

    bounds.size should be (2)
    bounds(0).lower should be (0)
    bounds(0).upper should be (4)
    bounds(1).lower should be (0)
    bounds(1).upper should be (2)
  }

  test("BoxCalculator should generate 8 combinations of splits") {
    val xBounds = new BoundsInOneDimension(0.0, 4.0, true).split (4)
    val yBounds = new BoundsInOneDimension(0.0, 2.0, true).split (2)

    val splitCombinations = BoxCalculator.generateCombinationsOfSplits(List (xBounds, yBounds), 1)

    splitCombinations.size should be (8)

  }

  test("BoxCalculator should split a box into 6 boxes") {

    val bigBox = new Box ((0.0, 3.0, true), (0.0, 2.0, true))
    val settings = new DbscanSettings ().withEpsilon(0.5)

    val smallBoxes = BoxCalculator.splitBoxIntoEqualBoxes(bigBox, 3, settings)

    smallBoxes.size should be (6)
  }

  test("BoxCalculator should split a box into 2 boxes") {
    val bigBox = new Box ((0.0, 2.0, true), (0.0, 1.0, true))
    val settings = new DbscanSettings ().withEpsilon(0.5)

    val smallBoxes = BoxCalculator.splitBoxIntoEqualBoxes(bigBox, 3, settings)

    smallBoxes.size should be (2)
  }

  test("BoxCalculator should split a larger box into a larger number of small boxes") {
    val settings = new DbscanSettings ().withEpsilon(0.5)
    val bigBox = new Box ((0.0, 3.0, true), (0.0, 2.0, true))
    val maxSplits = 3

    val smallBoxes = BoxCalculator.splitBoxIntoEqualBoxes(bigBox, maxSplits, settings).toArray
    smallBoxes.size should be (6)

    val biggerBox = bigBox.extendBySizeOfOtherBox(smallBoxes(0))

    biggerBox.bounds(0).lower should be (-0.5)
    biggerBox.bounds(0).upper should be (3.5)
    biggerBox.bounds(1).lower should be (-0.5)
    biggerBox.bounds(1).upper should be (2.5)

    val moreSmallBoxes = BoxCalculator.splitBoxIntoEqualBoxes(biggerBox, maxSplits+1, settings)

    moreSmallBoxes.size should be (12)
  }

  test ("BoxCalculator should find adjacent boxes correctly") {

    val b1 = new Box ((0.0, 1.0), (0.0, 1.0)).withId(1)
    val b2 = new Box ((1.0, 2.0), (0.0, 1.0)).withId(2)
    val b3 = new Box ((2.0, 3.0), (0.0, 1.0)).withId(3)
    val b4 = new Box ((0.0, 3.0), (1.0, 2.0)).withId(4)
    val allBoxes = b1 :: b2 :: b3 :: b4 :: Nil

    BoxCalculator.assignAdjacentBoxes (allBoxes)

    assert (b1.adjacentBoxes.contains(b2))
    assert (b1.adjacentBoxes.contains(b4))

    assert (b2.adjacentBoxes.contains(b1))
    assert (b2.adjacentBoxes.contains(b3))
    assert (b2.adjacentBoxes.contains(b4))

    assert (b3.adjacentBoxes.contains(b2))
    assert (b3.adjacentBoxes.contains(b4))

    assert (b4.adjacentBoxes.contains(b1))
    assert (b4.adjacentBoxes.contains(b2))
    assert (b4.adjacentBoxes.contains(b3))

    val distinctAdjacentBoxIds = BoxCalculator.generateDistinctPairsOfAdjacentBoxIds(allBoxes).toArray

    assert (distinctAdjacentBoxIds.length == 5)
    assert (distinctAdjacentBoxIds.contains((b1.boxId, b2.boxId)))
    assert (distinctAdjacentBoxIds.contains((b1.boxId, b4.boxId)))
    assert (distinctAdjacentBoxIds.contains((b2.boxId, b3.boxId)))
    assert (distinctAdjacentBoxIds.contains((b2.boxId, b4.boxId)))
    assert (distinctAdjacentBoxIds.contains((b3.boxId, b4.boxId)))
  }

  test ("BoxCalculator should generate embracing box") {
    val rootBox = new Box((0.0, 1.0), (0.0, 1.0)).withId(10)
    val boxBelow = new Box ((0.0, 1.0), (-1.0, 0.0)).withId (11)
    val boxAbove = new Box ((0.0, 1.0), (1.0, 2.0)).withId (12)
    val boxOnLeft = new Box ((-1.0, 0.0), (0.0, 1.0)).withId (14)
    val boxOnRight = new Box ((1.0, 2.0), (0.0, 1.0)).withId (9) // Id is less than root box id, so it should not
                                                                  // be included into the embracing box

    BoxCalculator.assignAdjacentBoxes(rootBox :: boxBelow :: boxAbove :: boxOnLeft :: boxOnRight :: Nil)

    assert (rootBox.adjacentBoxes.size == 4)

    val embracingBox = BoxCalculator.generateEmbracingBoxFromAdjacentBoxes(rootBox)
    assert (embracingBox.bounds(0).equals(new BoundsInOneDimension(-1.0, 1.0)))
    assert (embracingBox.bounds(1).equals(new BoundsInOneDimension(-1.0, 2.0)))
  }
}

