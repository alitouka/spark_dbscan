package org.alitouka.spark.dbscan

import org.alitouka.spark.dbscan.spatial.Point


class PointSuite extends SuiteBase {
  test ("Point should determine if it is 'greater' or 'less' than other point") {

    val pt1 = new Point (1, 2, 3)
    val pt2 = new Point (1, 2, 3)
    val pt3 = new Point (1, 3, 2)
    val pt4 = new Point (1, 1, 4)

    pt1.compareTo(pt2) should be (0)

    assert (pt1 < pt3)
    assert (pt1 > pt4)
  }

  test ("Point should implement equality comparison") {
    val pt1 = create2DPoint(1, 1)
    val pt2 = create2DPoint(1, 2)
    val pt3 = create2DPoint(1, 2)

    val twoPoints = Array (pt1, pt2)

    pt2 should equal (pt3)
    assert (pt2 == pt3)
    pt1 should not equal (pt2)
    twoPoints should contain (pt3)
  }
}
