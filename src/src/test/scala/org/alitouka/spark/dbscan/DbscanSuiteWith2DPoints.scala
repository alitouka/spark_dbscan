package org.alitouka.spark.dbscan


class DbscanSuiteWith2DPoints extends DbscanSuiteBase {



  test ("DBSCAN algorithm should coalesce all points into one cluster") {

    val settings = new DbscanSettings ().withEpsilon (0.8).withNumberOfPoints (4)
    val clusteringResult = Dbscan.train (dataset1, settings)

    val clusters = groupPointsByCluster (clusteringResult)
    clusters.size should be (1)

    val theOnlyCluster = clusters (0)

    val corePoint = create2DPoint (0.5, 0.5)
    theOnlyCluster.size should be (4)
    theOnlyCluster should contain (corePoint.coordinates)
  }

  test ("DBSCAN algorithm shouldfind one 4-point cluster and one noise point") {

    val settings = new DbscanSettings ().withEpsilon (0.8).withNumberOfPoints (4)
    val clusteringResult = Dbscan.train (dataset1_1, settings)

    val clusters = groupPointsByCluster (clusteringResult)
    clusters.size should be (1)

    val theOnlyCluster = clusters (0)
    val noise = clusteringResult.noisePoints.collect ()

    val corePoint = create2DPoint (0.5, 0.5)
    theOnlyCluster.size should be (4)
    theOnlyCluster should contain (corePoint.coordinates)

    noise.size should be (1)
    noise should contain (create2DPoint (0.5, 5.0))
  }

  test ("DBSCAN algorithm shouldfind 2 clusters, and one of the points may belong to any cluster") {

    val settings = new DbscanSettings ().withEpsilon (0.8).withNumberOfPoints (4)
    val clusteringResult = Dbscan.train (dataset2, settings)

    val clusters = groupPointsByCluster (clusteringResult)
    clusters.size should be (2)


    val corePoint1 = create2DPoint (0.5, 0.5)
    val corePoint2 = create2DPoint (1.5, 0.5)
    val ambiguousPoint = create2DPoint (1.0, 0.5)

    val cluster1 = findClusterWithPoint (clusters, corePoint1).get.toArray
    val cluster2 = findClusterWithPoint (clusters, corePoint2).get.toArray

    if (cluster1.contains (ambiguousPoint.coordinates)) {
      cluster1.size should be (4)
      cluster2.size should be (3)
    }
    else {
      cluster1.size should be (3)
      cluster2.size should be (4)
      cluster2 should contain (ambiguousPoint.coordinates)
    }
  }

  test ("DBSCAN algorithm shouldfind 2 clusters and 3 noise points") {
    val settings = new DbscanSettings ().withEpsilon (2.0).withNumberOfPoints (3)
    val clusteringResult = Dbscan.train (dataset0, settings)
    val clusters = groupPointsByCluster(clusteringResult)
    clusters.size should be (2)

    val cluster1 = findClusterWithPoint (clusters, create2DPoint (1.0, 1.0)).get.toArray
    val cluster2 = findClusterWithPoint (clusters, create2DPoint (5.0, 4.0)).get.toArray
    val noise = clusteringResult.noisePoints.collect ()

    cluster1.size should be (4)
    cluster2.size should be (3)
    noise.size should be (3)
  }

  test ("DBSCAN algorithm shouldfind 2 clusters and 2 noise points") {
    val settings = new DbscanSettings ().withEpsilon (0.8).withNumberOfPoints (3)
    val clusteringResult = Dbscan.train (dataset4, settings)
    val clusters = groupPointsByCluster(clusteringResult)
    clusters.size should be (2)

    val cluster1 = findClusterWithPoint (clusters, create2DPoint (0.0, 0.0)).get.toArray
    val cluster2 = findClusterWithPoint (clusters, create2DPoint (4.0, 0.0)).get.toArray
    val noise = clusteringResult.noisePoints.collect ()

    cluster1 should not equal (cluster2)
    cluster1.size should be (14)
    cluster2.size should be (14)
    noise.size should be (2)
  }

  test ("DBSCAN algorithm shouldfind 2 clusters, where 2 points can belong to any cluster") {
    val settings = new DbscanSettings ().withEpsilon (1.0).withNumberOfPoints (4)
    val clusteringResult = Dbscan.train (dataset4, settings)
    val clusters = groupPointsByCluster(clusteringResult)
    clusters.size should be (2) // 2 clusters, no noise

    val ambiguousPoint1 = create2DPoint (2.0, 0.0)
    val ambiguousPoint2 = create2DPoint(2.0, 2.0)

    val cluster1 = findClusterWithPoint (clusters, create2DPoint (0.0, 0.0)).get.toArray
    val cluster2 = findClusterWithPoint (clusters, create2DPoint (4.0, 0.0)).get.toArray

    var expectedCluster1Size = 14
    var expectedCluster2Size = 14

    if (cluster1.contains(ambiguousPoint1.coordinates)) {
      expectedCluster1Size += 1
    }
    else {
      expectedCluster2Size += 1
    }

    if (cluster1.contains(ambiguousPoint2.coordinates)) {
      expectedCluster1Size += 1
    }
    else {
      expectedCluster2Size += 1
    }

    cluster1.size should be (expectedCluster1Size)
    cluster2.size should be (expectedCluster2Size)
  }

  test ("DBSCAN* algorithm should find one 1-point cluster and 3 noise points") {

    val settings = new DbscanSettings ().withEpsilon(0.8).withNumberOfPoints(4).withTreatBorderPointsAsNoise(true)
    val clusteringResult = Dbscan.train (dataset1, settings)

    val clusters = groupPointsByCluster (clusteringResult)
    clusters.size should be (1)

    val corePoint = create2DPoint (0.5, 0.5)
    val theOnlyCluster = findClusterWithPoint (clusters, corePoint).get.toArray
    theOnlyCluster.size should be (1)


    val noise = clusteringResult.noisePoints.collect ()
    noise.size should be (3)
  }

  test ("DBSCAN* algorithm shouldfind one 1-point cluster and 4 noise points") {

    val settings = new DbscanSettings ().withEpsilon(0.8).withNumberOfPoints(4).withTreatBorderPointsAsNoise(true)
    val clusteringResult = Dbscan.train (dataset1_1, settings)

    val clusters = groupPointsByCluster (clusteringResult)
    clusters.size should be (1)

    val corePoint = create2DPoint (0.5, 0.5)
    val theOnlyCluster = findClusterWithPoint (clusters, corePoint).get.toArray
    theOnlyCluster.size should be (1)

    val noise = clusteringResult.noisePoints.collect ()
    noise.size should be (4)
  }

  test ("DBSCAN* algorithm shouldfind 2 1-point clusters and 5 noise points") {

    val settings = new DbscanSettings ().withEpsilon(0.8).withNumberOfPoints(4).withTreatBorderPointsAsNoise(true)
    val clusteringResult = Dbscan.train (dataset2, settings)

    val clusters = groupPointsByCluster (clusteringResult)
    clusters.size should be (2)

    val corePoint1 = create2DPoint (0.5, 0.5)
    val corePoint2 = create2DPoint (1.5, 0.5)

    val cluster1 = findClusterWithPoint (clusters, corePoint1).get.toArray
    val cluster2 = findClusterWithPoint (clusters, corePoint2).get.toArray
    val noise = clusteringResult.noisePoints.collect ()

    cluster1.size should be (1)
    cluster2.size should be (1)
    noise.size should be (5)
  }

  test ("DBSCAN* algorithm shouldfind 2 clusters and 2 noise points") {

    val settings = new DbscanSettings ().withEpsilon(1.0).withNumberOfPoints(4).withTreatBorderPointsAsNoise(true)
    val clusteringResult = Dbscan.train (dataset4, settings)
    val clusters = groupPointsByCluster(clusteringResult)
    clusters.size should be (2)


    val cluster1 = findClusterWithPoint (clusters, create2DPoint (0.0, 0.0)).get.toArray
    val cluster2 = findClusterWithPoint (clusters, create2DPoint (4.0, 0.0)).get.toArray
    val noise = clusteringResult.noisePoints.collect ()

    cluster1.size should be (14)
    cluster2.size should be (14)
    noise.size should be (2)
  }
}

