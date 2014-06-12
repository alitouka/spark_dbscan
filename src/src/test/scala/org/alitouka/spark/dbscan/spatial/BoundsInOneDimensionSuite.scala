package org.alitouka.spark.dbscan.spatial

import org.alitouka.spark.dbscan.SuiteBase

class BoundsInOneDimensionSuite extends SuiteBase {

  test ("BoundsInOneDimension should be splitted into 4 equal parts") {
    val b = new BoundsInOneDimension (0, 2, true)

    val split = b.split(4)
    split.size should be (4)

    split(0).lower should be (0)
    split(0).upper should be (0.5)
    split(0).includeHigherBound should be (false)

    split(1).lower should be (0.5)
    split(1).upper should be (1)
    split(1).includeHigherBound should be (false)

    split(2).lower should be (1)
    split(2).upper should be (1.5)
    split(2).includeHigherBound should be (false)

    split(3).lower should be (1.5)
    split(3).upper should be (2)
    split(3).includeHigherBound should be (true)
  }

  test ("BoundsInOneDimension should be splitted into 3 or 4 parts depending on epsilon") {
    val b = new BoundsInOneDimension (0, 4, true)

    val split4 = b.split (4, 1)
    split4.size should be (4)

    val split3 = b.split (4, 1.2)
    split3.size should be (3)
  }

  test("BoundsInOneDimension should be extended by 0.5 on each side") {
    val b1 = new BoundsInOneDimension(0.0, 5.0, true)
    val b2 = new BoundsInOneDimension(1.0, 2.0)

    val b3 = b1.extend(b2)

    b3.lower should be (-0.5)
    b3.upper should be (5.5)
    assert (b3.includeHigherBound)
  }

  test ("BoundsInOneDimension should implement equality comparison correctly") {
    val b1 = new BoundsInOneDimension (1.0, 2.0, true)
    val b2 = new BoundsInOneDimension (1.0, 2.0, false)
    val b3 = new BoundsInOneDimension (1.1, 2.0, true)
    val b4 = new BoundsInOneDimension (1.0, 2.2, true)
    val b5 = new BoundsInOneDimension (1.1, 2.2, true)
    val b6 = new BoundsInOneDimension (1.0, 2.0, true)

    b1 should equal (b6)

    b1 should not equal (b2)
    b1 should not equal (b3)
    b1 should not equal (b4)
    b1 should not equal (b5)
  }
}
