package org.alitouka.spark.dbscan

import org.apache.spark.SparkContext
import org.alitouka.spark.dbscan.spatial.Point

trait TestDatasets extends SuiteBase {

  val dataset0 = createRDDOfPoints (sc, (1.0, 1.0), (2.0, 1.0), (2.0, 2.0), (3.0, 2.0), (5.0, 4.0), (6.0, 4.0), (6.0, 5.0), (6.0, 1.0), (1.0, 5.0), (2.0, 5.0))
  val dataset0_1 = createRDDOfPoints (sc, (1.0, 1.0), (2.0, 1.0), (2.0, 2.0), (3.0, 2.0), (15.0, 14.0), (16.0, 14.0), (16.0, 15.0), (16.0, 11.0), (1.0, 5.0), (2.0, 5.0))
  val dataset1 = createRDDOfPoints (sc, (0.0, 0.0), (1.0, 0.0), (0.5, 1.0), (0.5, 0.5) )
  val dataset1_1 = createRDDOfPoints (sc, (0.0, 0.0), (1.0, 0.0), (0.5, 1.0), (0.5, 0.5), (0.5, 5.0) )
  val dataset2 = createRDDOfPoints (sc, (0.0, 0.0), (0.0, 1.0), (0.5, 0.5), (1.0, 0.5), (1.5, 0.5), (2.0, 0.0), (2.0, 1.0) )
  var dataset4 = createRDDOfPoints (sc, (0.0, 1.0), (1.0, 1.0), (3.0, 1.0), (4.0, 1.0), (0.5, 0.5), (0.5, 1.5), (3.5, 0.5), (3.5, 1.5),
    (0.0, 3.0), (1.0, 3.0), (3.0, 3.0), (4.0, 3.0), (0.5, 2.5), (3.5, 2.5),
    (0.0, 4.0), (1.0, 4.0), (3.0, 4.0), (4.0, 4.0), (0.5, 3.5), (3.5, 3.5)
  )

  dataset4 ++= sc.parallelize((0 to 4).map ( x => create2DPoint (x, 0.0).withPointId (100+x) ))
  dataset4 ++= sc.parallelize((0 to 4).map ( x => create2DPoint (x, 2.0).withPointId (200+x) ))

}
