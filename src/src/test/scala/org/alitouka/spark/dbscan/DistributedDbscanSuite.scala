package org.alitouka.spark.dbscan

import org.alitouka.spark.dbscan.spatial._
import org.alitouka.spark.dbscan.spatial.rdd.{PointsPartitionedByBoxesRDD, BoxPartitioner}
import org.alitouka.spark.dbscan.spatial.BoundsInOneDimension._

class DistributedDbscanSuite extends DbscanSuiteBase with TestDatasets {

  val ordering = implicitly[Ordering[PointSortKey]]

  val bigBox = new Box ((-Double.MaxValue, Double.MaxValue), (-Double.MaxValue, Double.MaxValue))
  val boxes = BoxPartitioner.assignPartitionIdsToBoxes(List (bigBox))
  val broadcastBoxes = sc.broadcast(boxes)
  val broadcastNumberOfDimensions = sc.broadcast(2)
  val defaultBoundingBox = new Box ((0.0, 5.0, true), (0.0, 5.0, true))

  val pointsInDifferentBoxes = Array (create2DPoint (0.1, 0.9, 1, 4, 1, 1), create2DPoint (0.2, 0.9, 2, 4, 1, 2),
    create2DPoint (0.1, 1.1, 3, 4, 2, 3), create2DPoint (0.2, 1.1, 4, 4, 2, 4),
    create2DPoint (2.1, 0.9, 5, 3, 1, 5), create2DPoint (2.1, 1.1, 6, 3, 2, 6), create2DPoint (2.1, 0.5, 55, 3, 1, 5),
    create2DPoint (5.1, 0.9, 7, 3, 1, 7), create2DPoint (5.1, 1.1, 8, 2, 2, DbscanModel.UndefinedCluster),
    create2DPoint (5.1, 0.0, 77, 2, 1, 7))

  test ("DistributedDbscan.generateMappings should map 2 clusters and 1 border point") {
    val settings = new DbscanSettings ().withEpsilon (1).withNumberOfPoints (3)

    val impl = new DistributedDbscan(settings)

    val (mappings, borderPoints) = impl.generateMappings(pointsInDifferentBoxes)

    mappings.size should be (2)
    borderPoints.size should be (1)

    val mapping1 = mappings.find ( _._1.contains(1) ).get._1
    val mapping2 = mappings.find ( _._1.contains(5) ).get._1


    mapping1.size should be (4)
    mapping2.size should be (2)


    mapping1 should contain (1)
    mapping1 should contain (2)
    mapping1 should contain (3)


    mapping2 should contain (6)

    borderPoints should contain ((8, 7))
  }

  test("DistributedDbscan.generateMappings should map 2 clusters and no border points") {
    val settings = new DbscanSettings ().withEpsilon (1).withNumberOfPoints (3).withTreatBorderPointsAsNoise(true)

    val impl = new DistributedDbscan(settings)

    val (mappings, borderPoints) = impl.generateMappings(pointsInDifferentBoxes)

    mappings.size should be (2)
    borderPoints.size should be (0)

    val mapping1 = mappings.find ( _._1.contains(1) ).get._1
    val mapping2 = mappings.find ( _._1.contains(5) ).get._1


    mapping1.size should be (4)
    mapping2.size should be (2)


    mapping1 should contain (1)
    mapping1 should contain (2)
    mapping1 should contain (3)

    mapping2 should contain (6)
  }

  test ("DistributedDbscan.reassignClusterId should group all points into 3 clusters") {
//    val settings = new DbscanSettings ().withEpsilon (1).withNumberOfPoints (3)
//
//    val impl = new DistributedDbscan(settings)
//
//    val (mappings, borderPoints) = impl.generateMappings(pointsInDifferentBoxes)
//
//    val clusteredIterator = pointsInDifferentBoxes.map ( pt => impl.reassignClusterId(pt, mappings, borderPoints) ).map ( pt => (new PointSortKey(pt), pt)).iterator
//
//    val (noise, clusters) = groupPointsAndSeparateNoiseFromClusters(clusteredIterator)
//
//    clusters.size should be (3)
//    noise.size should be (0)



  }

  test ("DistributedDbscan.findClustersInOnePartition should coalesce all points into one cluster") {

    val settings = new DbscanSettings ().withEpsilon (0.8).withNumberOfPoints (4)
    val sortedIterator = sortDataset (dataset1, settings)
    val impl = new DistributedDbscan(settings)
    val clusteredIterator = impl.findClustersInOnePartition(sortedIterator, defaultBoundingBox)
    val (noise, clusters) = groupPointsAndSeparateNoiseFromClusters(clusteredIterator)

    clusters.size should be (1)

    val theOnlyCluster = clusters (0)

    val corePoint = create2DPoint (0.5, 0.5)
    theOnlyCluster.size should be (4)
    theOnlyCluster should contain (corePoint)
  }

  test("DistributedDbscan.findClustersInOnePartition should find one 4-point cluster and one noise point") {

    val settings = new DbscanSettings ().withEpsilon (0.8).withNumberOfPoints (4)
    val sortedIterator = sortDataset (dataset1_1, settings)
    val impl = new DistributedDbscan(settings)
    val clusteredIterator = impl.findClustersInOnePartition(sortedIterator, defaultBoundingBox)
    val (noise, clusters) = groupPointsAndSeparateNoiseFromClusters(clusteredIterator)

    clusters.size should be (1)

    val theOnlyCluster = clusters (0)
    val corePoint = create2DPoint (0.5, 0.5)
    theOnlyCluster.size should be (4)
    theOnlyCluster should contain (corePoint)

    noise.size should be (1)
    noise should contain (create2DPoint (0.5, 5.0))
  }

  test("DistributedDbscan.findClustersInOnePartition should find 2 clusters, and one of the points may belong to any cluster") {

    val settings = new DbscanSettings ().withEpsilon (0.8).withNumberOfPoints (4)

    val sortedIterator = sortDataset (dataset2, settings)
    val impl = new DistributedDbscan(settings)
    val clusteredIterator = impl.findClustersInOnePartition(sortedIterator, defaultBoundingBox)
    val (noise, clusters) = groupPointsAndSeparateNoiseFromClusters(clusteredIterator)

    clusters.size should be (2)

    val corePoint1 = create2DPoint (0.5, 0.5)
    val corePoint2 = create2DPoint (1.5, 0.5)
    val ambiguousPoint = create2DPoint (1.0, 0.5)

    val cluster1 = findClusterWithPoint2 (clusters, corePoint1).get.toArray
    val cluster2 = findClusterWithPoint2 (clusters, corePoint2).get.toArray

    if (cluster1.contains (ambiguousPoint)) {
      cluster1.size should be (4)
      cluster2.size should be (3)
    }
    else {
      cluster1.size should be (3)
      cluster2.size should be (4)
      cluster2 should contain (ambiguousPoint)
    }
  }

  test("DistributedDbscan.findClustersInOnePartition should find 2 clusters and 3 noise points") {
    val settings = new DbscanSettings ().withEpsilon (2.0).withNumberOfPoints (3)

    val sortedIterator = sortDataset (dataset0_1, settings)
    val impl = new DistributedDbscan(settings)
    val clusteredIterator = impl.findClustersInOnePartition(sortedIterator,
      new Box ((0.0, 16.0, true), (0.0, 16.0, true)))

    val (noise, clusters) = groupPointsAndSeparateNoiseFromClusters(clusteredIterator)

    clusters.size should be (2)

    val cluster1 = findClusterWithPoint2 (clusters, create2DPoint (1.0, 1.0)).get.toArray
    val cluster2 = findClusterWithPoint2 (clusters, create2DPoint (15.0, 14.0)).get.toArray

    cluster1.size should be (4)
    cluster2.size should be (3)
    noise.size should be (3)
  }

  test("DistributedDbscan.findClustersInOnePartition should find 2 clusters and 2 noise points") {
    val settings = new DbscanSettings ().withEpsilon (0.8).withNumberOfPoints (3)

    val sortedIterator = sortDataset (dataset4, settings)
    val impl = new DistributedDbscan(settings)
    val clusteredIterator = impl.findClustersInOnePartition(sortedIterator, defaultBoundingBox)
    val (noise, clusters) = groupPointsAndSeparateNoiseFromClusters(clusteredIterator)

    clusters.size should be (2)

    val cluster1 = findClusterWithPoint2 (clusters, create2DPoint (0.0, 0.0)).get.toArray
    val cluster2 = findClusterWithPoint2 (clusters, create2DPoint (4.0, 0.0)).get.toArray

    cluster1 should not equal (cluster2)
    cluster1.size should be (14)
    cluster2.size should be (14)
    noise.size should be (2)
  }

  test("DistributedDbscan.findClustersInOnePartition should find 2 clusters, where 2 points can belong to any cluster") {
    val settings = new DbscanSettings ().withEpsilon (1.0).withNumberOfPoints (4)
    val sortedIterator = sortDataset (dataset4, settings)
    val impl = new DistributedDbscan(settings)
    val clusteredIterator = impl.findClustersInOnePartition(sortedIterator, defaultBoundingBox)
    val (noise, clusters) = groupPointsAndSeparateNoiseFromClusters(clusteredIterator)

    clusters.size should be (2) // 2 clusters, no noise

    val ambiguousPoint1 = create2DPoint (2.0, 0.0)
    val ambiguousPoint2 = create2DPoint(2.0, 2.0)

    val cluster1 = findClusterWithPoint2 (clusters, create2DPoint (0.0, 0.0)).get.toArray
    val cluster2 = findClusterWithPoint2 (clusters, create2DPoint (4.0, 0.0)).get.toArray

    var expectedCluster1Size = 14
    var expectedCluster2Size = 14

    if (cluster1.contains(ambiguousPoint1)) {
      expectedCluster1Size += 1
    }
    else {
      expectedCluster2Size += 1
    }

    if (cluster1.contains(ambiguousPoint2)) {
      expectedCluster1Size += 1
    }
    else {
      expectedCluster2Size += 1
    }

    cluster1.size should be (expectedCluster1Size)
    cluster2.size should be (expectedCluster2Size)
  }

  test ("DistributedDbscan.findClustersInOnePartition * should find one 1-point cluster and 3 noise points") {

    val settings = new DbscanSettings ().withEpsilon(0.8).withNumberOfPoints(4).withTreatBorderPointsAsNoise(true)

    val sortedIterator = sortDataset (dataset1, settings)
    val impl = new DistributedDbscan(settings)
    val clusteredIterator = impl.findClustersInOnePartition(sortedIterator, defaultBoundingBox)
    val (noise, clusters) = groupPointsAndSeparateNoiseFromClusters(clusteredIterator)

    clusters.size should be (1)

    val corePoint = create2DPoint (0.5, 0.5)
    val theOnlyCluster = findClusterWithPoint2 (clusters, corePoint).get.toArray
    theOnlyCluster.size should be (1)

    noise.size should be (3)
  }

  test("DistributedDbscan.findClustersInOnePartition *find one 1-point cluster and 4 noise points") {

    val settings = new DbscanSettings ().withEpsilon(0.8).withNumberOfPoints(4).withTreatBorderPointsAsNoise(true)

    val sortedIterator = sortDataset (dataset1_1, settings)
    val impl = new DistributedDbscan(settings)
    val clusteredIterator = impl.findClustersInOnePartition(sortedIterator, defaultBoundingBox)
    val (noise, clusters) = groupPointsAndSeparateNoiseFromClusters(clusteredIterator)

    clusters.size should be (1)

    val corePoint = create2DPoint (0.5, 0.5)
    val theOnlyCluster = findClusterWithPoint2 (clusters, corePoint).get.toArray
    theOnlyCluster.size should be (1)

    noise.size should be (4)
  }

  test("DistributedDbscan.findClustersInOnePartition *find 2 1-point clusters and 5 noise points") {

    val settings = new DbscanSettings ().withEpsilon(0.8).withNumberOfPoints(4).withTreatBorderPointsAsNoise(true)

    val sortedIterator = sortDataset (dataset2, settings)
    val impl = new DistributedDbscan(settings)
    val clusteredIterator = impl.findClustersInOnePartition(sortedIterator, defaultBoundingBox)
    val (noise, clusters) = groupPointsAndSeparateNoiseFromClusters(clusteredIterator)

    clusters.size should be (2)

    val corePoint1 = create2DPoint (0.5, 0.5)
    val corePoint2 = create2DPoint (1.5, 0.5)

    val cluster1 = findClusterWithPoint2 (clusters, corePoint1).get.toArray
    val cluster2 = findClusterWithPoint2 (clusters, corePoint2).get.toArray


    cluster1.size should be (1)
    cluster2.size should be (1)
    noise.size should be (5)
  }

  test("DistributedDbscan.findClustersInOnePartition *find 2 clusters and 2 noise points") {

    val settings = new DbscanSettings ().withEpsilon(1.0).withNumberOfPoints(4).withTreatBorderPointsAsNoise(true)
    val sortedIterator = sortDataset (dataset4, settings)
    val impl = new DistributedDbscan(settings)
    val clusteredIterator = impl.findClustersInOnePartition(sortedIterator, defaultBoundingBox)
    val (noise, clusters) = groupPointsAndSeparateNoiseFromClusters(clusteredIterator)

    clusters.size should be (2)

    val cluster1 = findClusterWithPoint2 (clusters, create2DPoint (0.0, 0.0)).get.toArray
    val cluster2 = findClusterWithPoint2 (clusters, create2DPoint (4.0, 0.0)).get.toArray

    cluster1.size should be (14)
    cluster2.size should be (14)
    noise.size should be (2)
  }



  private def sortDataset (data: RawDataSet, settings: DbscanSettings) = {

    val ds2 = PointsPartitionedByBoxesRDD (data, dbscanSettings = settings)
    val ds3 = new DistanceAnalyzer (settings).countNeighborsForEachPoint(ds2)

      ds3
      .collect()
      .sortWith ((x, y) => ordering.lt(x._1, y._1))
      .iterator
  }

  private def groupPointsAndSeparateNoiseFromClusters (it: Iterator[(PointSortKey, Point)]) = {
    val clustersAndNoise = groupPointsByCluster (it)
    val (noise, clusters) = clustersAndNoise.partition ( x => x.exists( p => p.clusterId == DbscanModel.NoisePoint || p.clusterId == DbscanModel.UndefinedCluster ) )

    if (!noise.isEmpty) {
      (noise.flatMap ( x => x) , clusters)
    }
    else {
      (noise, clusters)
    }
  }

  private def create2DPoint (x: Double, y: Double, id: PointId, neighbors: Int, boxId: BoxId, cluster: ClusterId): Point = {
    new Point (new PointCoordinates (Array (x, y)), id, boxId, Math.sqrt (x*x+y*y), neighbors, cluster)
  }


}
