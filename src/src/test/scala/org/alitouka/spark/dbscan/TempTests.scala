package org.alitouka.spark.dbscan

import org.apache.spark.SparkContext._
import scala.collection.mutable.WrappedArray.ofDouble

class TempTests extends DbscanSuiteBase {

  test ("It should be possible to check whether points are in the same cluster") {

    val pt10 = create2DPoint (1, 1)
    val pt20 = create2DPoint (2, 1)
    val pt30 = create2DPoint (2, 2)
    val pt51 = create2DPoint (3, 2)

    val pt40 = create2DPoint (5, 4)
    val pt50 = create2DPoint (6, 4)
    val pt11 = create2DPoint (6, 5)

    val data = sc.parallelize(Array (pt10, pt20, pt30, pt51, pt40, pt50, pt11))
    val clusters = sc.parallelize(Array (1, 1, 1, 1, 2, 2, 2))

    val clusteredData = data.zip (clusters) // This is how data will be represented in DbscanModel

    val pointsGroupedByCluster = clusteredData.map ( x => (x._2, x._1.coordinates)).groupByKey().map ( _._2 ).collect ()
    pointsGroupedByCluster.foreach (println)

    val cluster1 = findClusterWithPoint (pointsGroupedByCluster, pt10).get.toArray

    cluster1.size should be (4)
    cluster1 should contain (pt20.coordinates)
    cluster1 should contain (pt30.coordinates)
    cluster1 should contain (pt51.coordinates)

    val cluster2 = findClusterWithPoint (pointsGroupedByCluster, pt40).get.toArray
    cluster2.size should be (3)
    cluster2 should contain (pt50.coordinates)
    cluster2 should contain (pt11.coordinates)

    val notExistingCluster = findClusterWithPoint (pointsGroupedByCluster, create2DPoint (0, 0))
    notExistingCluster should be (None)

    val cluster3 = findClusterWithPoint (pointsGroupedByCluster, pt30).get.toArray
    cluster3 should equal (cluster1)

  }

  test ("It should be possible to extract double values from a comma-separated line") {
    val line = "1.0,2.0,3.0,4.0,5.0"
    val values = new ofDouble (line.split(",").map( _.toDouble ))

    values.length should be (5)

    for (i <- 1 to 5) {
      values should contain (i.toDouble)
    }
  }

  test ("Keeping only pairs with keys should keep 2 pairs") {
    val someData = sc.parallelize(Array ((1L, "one"), (2L, "two"), (3L, "three")))
    val keysToLeave = sc.parallelize (Array (2L, 3L))

    val pairsKept = Dbscan.keepOnlyPairsWithKeys(someData, keysToLeave).collect ()

    pairsKept.size should be (2)
    pairsKept should contain ( (2L, "two") )
    pairsKept should contain ( (3L, "three") )
  }
}
