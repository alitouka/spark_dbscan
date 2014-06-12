package org.alitouka.spark.dbscan

class PredictionSuite extends DbscanSuiteBase {

  val clusteredPoints = sc.parallelize (Array ( create2DPoint(0.0, 0.0).withClusterId(1),
                                              create2DPoint(1.0, 0.0).withClusterId(1),
                                              create2DPoint(0.0, 1.0).withClusterId(1),
                                              create2DPoint(1.0, 1.0).withClusterId(1),

                                              create2DPoint(3.0, 0.0).withClusterId(2),
                                              create2DPoint(4.0, 0.0).withClusterId(2),
                                              create2DPoint(3.0, 1.0).withClusterId(2),
                                              create2DPoint(4.0, 1.0).withClusterId(2),

                                              create2DPoint(1.0, 3.0).withClusterId(DbscanModel.NoisePoint),
                                              create2DPoint(3.0, 3.0).withClusterId(DbscanModel.NoisePoint)
  ))


  val settings1 = new DbscanSettings ().withEpsilon(1.5).withNumberOfPoints(3)
  val model1 = new DbscanModel (clusteredPoints, settings1)

  val settings2 = new DbscanSettings ().withEpsilon(1.5).withNumberOfPoints(4).withTreatBorderPointsAsNoise(true)
  val model2 = new DbscanModel (clusteredPoints, settings2)


  test ("DbscanModel should assign a point to cluster 1") {

    val predictedClusterId = model1.predict(create2DPoint (0.5, 0.5))

    predictedClusterId should equal (1L)
  }

  test ("DbscanModel should assign a point to any of the clusters") {
    val predictedClusterId = model1.predict(create2DPoint (2.0, 0.5))

    assert (predictedClusterId == 1L || predictedClusterId == 2L)
  }

  test ("DbscanModel should assign a point to cluster 2") {
    val predictedClusterId = model1.predict(create2DPoint (4.5, 0.5))

    predictedClusterId should equal (2L)
  }

  test ("DbscanModel should mark a point as noise") {
    val predictedClusterId = model1.predict(create2DPoint (0.0, 3.0))

    predictedClusterId should equal (DbscanModel.NoisePoint)
  }

  test ("DbscanModel should mark another point as noise") {
    val predictedClusterId = model1.predict(create2DPoint (7.0, 7.0))

    predictedClusterId should equal (DbscanModel.NoisePoint)
  }

  test ("DbscanModel should discover a new cluster") {
    val predictedClusterId = model1.predict(create2DPoint (2.0, 3.0))

    predictedClusterId should equal (DbscanModel.NewCluster)
  }
  
  test ("DbscanModel should mark a border point as noise") {
    val predictedClusterId = model2.predict(create2DPoint (4.5, 0.5))

    predictedClusterId should equal (DbscanModel.NoisePoint)
  }

  test ("DbscanModel should mark an ambigouos point as noise") {
    val predictedClusterId = model2.predict(create2DPoint (4.5, 0.5))

    predictedClusterId should equal (DbscanModel.NoisePoint)
  }

}
