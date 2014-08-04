package org.alitouka.spark.dbscan.spatial

import org.alitouka.spark.dbscan.{DbscanSettings, SuiteBase}


class BoxSuite extends SuiteBase {
  test ("Box should always report that points lie within it if they lie between its bounds") {

    val box = new Box ((0.0, 1.0), (0.0, 2.0))

    assert (box.isPointWithin(new Point (0.5, 1.0)))
    assert (box.isPointWithin(new Point (0.0001, 0.0001)))
    assert (box.isPointWithin(new Point (0.9999, 1.9999)))
  }

  test("Box should report that points lying on its lower bounds are also within it") {
    val box = new Box ((0.0, 1.0), (0.0, 2.0))

    assert (box.isPointWithin(new Point (0.0, 0.0)))
    assert (box.isPointWithin(new Point (0.0, 1.0)))
    assert (box.isPointWithin(new Point (0.5, 0.0)))
  }

  test("Box should report that points lying on its upper bounds are outside of it, unless configured otherwise") {
    val box1 = new Box ((0.0, 1.0), (0.0, 2.0))

    assert (!box1.isPointWithin(new Point (1.0, 1.0)))
    assert (!box1.isPointWithin(new Point (1.0, 2.0)))
    assert (!box1.isPointWithin(new Point (0.5, 2.0)))

    val box2 = new Box ((0.0, 1.0, true), (0.0, 2.0)) // including upper bound for the 1st dimension

    assert (box2.isPointWithin(new Point (1.0, 1.0)))
    assert (!box2.isPointWithin(new Point (1.0, 2.0)))
    assert (!box2.isPointWithin(new Point (0.5, 2.0)))

    val box3 = new Box ((0.0, 1.0), (0.0, 2.0, true)) // including upper bound for the 2nd dimension

    assert (!box3.isPointWithin(new Point (1.0, 1.0)))
    assert (!box3.isPointWithin(new Point (1.0, 2.0)))
    assert (box3.isPointWithin(new Point (0.5, 2.0)))

    val box4 = new Box ((0.0, 1.0, true), (0.0, 2.0, true)) // including upper bound for both dimensions

    assert (box4.isPointWithin(new Point (1.0, 1.0)))
    assert (box4.isPointWithin(new Point (1.0, 2.0)))
    assert (box4.isPointWithin(new Point (0.5, 2.0)))
  }

  test("Box should report points lying outside of its bound are actually outside") {
    val box = new Box ((0.0, 1.0), (0.0, 2.0))

    assert(!box.isPointWithin(new Point (-1.0, -1.0)))
    assert(!box.isPointWithin(new Point (-1.0, 1.0)))
    assert(!box.isPointWithin(new Point (-1.0, 3.0)))
    assert(!box.isPointWithin(new Point (0.5, 3.0)))
    assert(!box.isPointWithin(new Point (2.0, 3.0)))
    assert(!box.isPointWithin(new Point (2.0, 1.0)))
    assert(!box.isPointWithin(new Point (2.0, -1.0)))
    assert(!box.isPointWithin(new Point (0.5, -1.0)))
  }

  test("Box should split 4 times along its 1st dimension") {

    val bigBox = new Box ((0.0, 4.0), (0.0, 2.0), (1.0, 3.0))

    val smallBoxes = bigBox.splitAlongLongestDimension(4).toArray

    for (i <- 0 until 4) {
      smallBoxes(i).bounds(0).lower should be (i)
      smallBoxes(i).bounds(0).upper should be (i+1)

      smallBoxes(i).bounds(1).lower should be (0.0)
      smallBoxes(i).bounds(1).upper should be (2.0)

      smallBoxes(i).bounds(2).lower should be (1.0)
      smallBoxes(i).bounds(2).upper should be (3.0)
    }
  }

  test("Box should split 4 times along its 2nd dimension") {

    val bigBox = new Box ((0.0, 2.0), (0.0, 4.0), (1.0, 3.0))

    val smallBoxes = bigBox.splitAlongLongestDimension(4).toArray

    for (i <- 0 until 4) {
      smallBoxes(i).bounds(0).lower should be (0.0)
      smallBoxes(i).bounds(0).upper should be (2.0)

      smallBoxes(i).bounds(1).lower should be (i)
      smallBoxes(i).bounds(1).upper should be (i+1)

      smallBoxes(i).bounds(2).lower should be (1.0)
      smallBoxes(i).bounds(2).upper should be (3.0)
    }
  }

  test("Box should split 4 times along its 3rd dimension") {

    val bigBox = new Box ((0.0, 2.0), (1.0, 3.0), (0.0, 4.0))

    val smallBoxes = bigBox.splitAlongLongestDimension(4).toArray

    for (i <- 0 until 4) {
      smallBoxes(i).bounds(0).lower should be (0.0)
      smallBoxes(i).bounds(0).upper should be (2.0)

      smallBoxes(i).bounds(1).lower should be (1.0)
      smallBoxes(i).bounds(1).upper should be (3.0)

      smallBoxes(i).bounds(2).lower should be (i)
      smallBoxes(i).bounds(2).upper should be (i+1)
    }
  }

  test("Box should recognize whether it is big enough") {
    val settings = new DbscanSettings ().withEpsilon(1)

    val bigBox = new Box ((0.0, 2.0), (0.0, 3.0), (0.0, 4.0))
    assert (bigBox.isBigEnough(settings))

    val smallBox = new Box ((0.0, 2.0), (0.0, 1.0), (0.0, 4.0))
    assert (!smallBox.isBigEnough(settings))
  }

  test("Box should be extended by the size of other box") {
    val b1 = new Box ((0.0, 2.0, true), (0.0, 1.0, true))
    val b2 = new Box ((0.0, 2.0, true), (0.0, 1.0, true))

    val b3 = b1.extendBySizeOfOtherBox(b2)

    b3.bounds(0).lower should be (-1)
    b3.bounds(0).upper should be (3)
    b3.bounds(1).lower should be (-0.5)
    b3.bounds(1).upper should be (1.5)
  }

  test("Box should compare itself to other boxes") {
    val b1 = new Box ((0.0, 1.0, false), (0.0, 1.0, false))
    val b2 = new Box ((1.0, 2.0, true), (0.0, 1.0, true))
    val b3 = new Box ((0.0, 1.0, false), (1.0, 2.0, true))
    val b4 = new Box ((1.0, 2.0, true), (1.0, 2.0, true))

    val b5 = new Box ((1.0, 2.0, true), (0.0, 1.0, true))

    assert (b1 < b2)
    assert (b1 < b3)
    assert (b1 < b4)


    assert (b2 > b1)
    assert (b2 > b3)
    assert (b2 < b4)

    assert (b3 > b1)
    assert (b3 < b2)
    assert (b3 < b4)

    assert (b4 > b1)
    assert (b4 > b2)
    assert (b4 > b3)

    b2.compareTo (b5) should be (0)

    val boxes = Array (b4, b3, b2, b1).sorted

    boxes(0) should be (b1)
    boxes(1) should be (b3)
    boxes(2) should be (b2)
    boxes(3) should be (b4)
  }

  test("Box should construct a new box given a point and other box") {
    val pt = new Point (7.0, 6.0)
    val sizeBox = new Box ((0.0, 4.0), (0.0, 2.0))

    val testBox = Box (pt, sizeBox)

    testBox.centerPoint should be (pt)
    testBox.bounds(0).lower should be (5)
    testBox.bounds(0).upper should be (9)
    testBox.bounds(1).lower should be (5)
    testBox.bounds(1).upper should be (7)
  }

  test ("Box should correctly identify whether another box is adjacent to it") {
    val b1 = new Box ((0.0, 2.0), (0.0, 3.0))
    val b2 = new Box ((2.0, 3.0), (0.0, 1.0))
    val b3 = new Box ((2.0, 3.0), (1.0, 2.0))
    val b4 = new Box ((2.0, 3.0), (2.0, 3.0))
    val b5 = new Box ((2.0, 3.0), (3.0, 4.0))
    val b6 = new Box ((2.0, 3.0), (4.0, 5.0))
    val b7 = new Box ((0.5, 1.5), (0.5, 2.5))

    assert (b1.isAdjacentToBox(b2))
    assert (b1.isAdjacentToBox(b3))
    assert (b1.isAdjacentToBox(b4))
    assert (b1.isAdjacentToBox(b5))
    assert (!b1.isAdjacentToBox(b6))
    assert (b2.isAdjacentToBox(b3))
    assert (!b2.isAdjacentToBox(b4))
    assert (!b1.isAdjacentToBox(b7))
  }
}
