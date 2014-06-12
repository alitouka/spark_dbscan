package org.alitouka.spark.dbscan.util.math

import org.alitouka.spark.dbscan.SuiteBase
import DoubleComparisonOperations._

class DoubleComparisonOperationsSuite extends SuiteBase {

  test ("DoubleComparisonOperations treat numbers which are very close to each other as equal") {

    val lessThanEps = 5E-11

    val a = 42.0
    val b = 42.0 + lessThanEps
    val c = 42.0 - lessThanEps
    val d = 43.0


    assert (a != b)
    assert (a ~~ b)
    assert (a <~ d)
    assert (!(a ~~ d))
    assert (!(a >~ d))

    assert (d >= a)
    assert (d >~ a)
    assert (!(d <~ a))

    assert (b != a)
    assert (b > a)
    assert (b >~ a)
    assert (b <~ a)

    assert (c != a)
    assert (c < a)
    assert (c ~~ a)
    assert (c >~ a)
    assert (c <~ a)
  }

}
