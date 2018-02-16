package org.alitouka.spark.dbscan

import org.alitouka.spark.dbscan.spatial._
import org.apache.commons.math3.ml.distance.DistanceMeasure
import scala.collection.mutable.{ListBuffer, HashSet}
import org.apache.spark.rdd.RDD
import org.alitouka.spark.dbscan.spatial.rdd.{PointsInAdjacentBoxesRDD, PointsPartitionedByBoxesRDD, PartitioningSettings}
import scala.Some
import org.alitouka.spark.dbscan.util.commandLine.CommonArgs
import org.alitouka.spark.dbscan.util.debug.DebugHelper
import org.apache.spark.internal.Logging
import scala.collection.immutable.HashMap

/** Implementation of the DBSCAN algorithm which is capable of parallel processing of the input data.
  *
  *  Parallel processing consists of 4 high-level steps:
  *  - Density-based data partitioning
  *  - Calculating neighbors of each point
  *  - Clustering data in each partition separately
  *  - Merging clusters found in different partitions
  *
  * @param settings Parameters of the algorithm. See [[org.alitouka.spark.dbscan.DbscanSettings]]
  *                 for details
  * @param partitioningSettings Partitioning settings. See [[org.alitouka.spark.dbscan.spatial.rdd.PartitioningSettings]]
  *                             for details
  */
class DistributedDbscan (
  settings: DbscanSettings,
  partitioningSettings: PartitioningSettings = new PartitioningSettings ())
  extends Dbscan (settings, partitioningSettings) with DistanceCalculation with Logging {

  private [dbscan] implicit val distanceMeasure: DistanceMeasure = settings.distanceMeasure

  /** Runs the clustering algorithm
   *
   * @param data A data set to be clustered. See [[org.alitouka.spark.dbscan.RawDataSet]] for details
   * @return A [[org.alitouka.spark.dbscan.DbscanModel]] object which represents clustering results
   */
  override protected def run(data: RawDataSet): DbscanModel = {
    val distanceAnalyzer = new DistanceAnalyzer (settings)
    val partitionedData = PointsPartitionedByBoxesRDD (data, partitioningSettings, settings)

    DebugHelper.doAndSaveResult(data.sparkContext, "boxes") {
      path => {
        val boxBoundaries = partitionedData.boxes.map {
          box => {
            Array (box.bounds(0).lower, box.bounds(1).lower, box.bounds(0).upper, box.bounds(1).upper).mkString (",")
          }
        }.toArray
        
        data.sparkContext.parallelize(boxBoundaries).saveAsTextFile(path)
      }
    }

    val pointsWithNeighborCounts = distanceAnalyzer.countNeighborsForEachPoint(partitionedData)
    val broadcastBoxes = data.sparkContext.broadcast(partitionedData.boxes)

    val partiallyClusteredData = pointsWithNeighborCounts.mapPartitionsWithIndex (
      (partitionIndex, it) => {
        val boxes = broadcastBoxes.value
        val partitionBoundingBox = boxes.find ( _.partitionId == partitionIndex ).get
        findClustersInOnePartition(it, partitionBoundingBox)
      },
      preservesPartitioning = true
    )

    partiallyClusteredData.persist ()

    DebugHelper.doAndSaveResult(partiallyClusteredData.sparkContext, "partiallyClustered") {
      path => {
        partiallyClusteredData
          .map ( x => x._2.coordinates.mkString(",") + "," + x._2.clusterId)
          .saveAsTextFile(path)
      }
    }

    val completelyClusteredData = mergeClustersFromDifferentPartitions(partiallyClusteredData,
      partitionedData.boxes)

    new DbscanModel (completelyClusteredData, settings)
  }


  /** Finds clusters within one partition
    *
    * The algorithm implemented in this method is very similar to the one described in http://en.wikipedia.org/wiki/DBSCAN
    * except the fact that number of neighbors of each point is precomputed
    *
    * @param it An iterator which iterates through a collection of points accompanied by their sorting keys. At the moment
    *           when this method is called, [[org.alitouka.spark.dbscan.spatial.Point]] objects in the collection
    *           already contain all necessary metadata, including number of their neighbors
    *
    * @param boundingBox A [[org.alitouka.spark.dbscan.spatial.Box]] object which represents bounds of the whole dataset
    * @return An iterator which iterates through a collection of points accompanied by their sorting keys. Each point
    *         is assigned a cluster ID which can be found in its clusterId field
    */
  private [dbscan] def findClustersInOnePartition (it: Iterator[(PointSortKey, Point)],
    boundingBox: Box): Iterator[(PointSortKey, Point)] = {

    var tempPointId = 0
    val points = it.map {
      x => {
        tempPointId += 1
        val newPt = new PartiallyMutablePoint (x._2, tempPointId)

        (tempPointId, newPt)
      }
    }.toMap

    val partitionIndex = new PartitionIndex (boundingBox, settings, partitioningSettings)

    partitionIndex.populate(points.values)

    var startingPointWithId = findUnvisitedCorePoint(points, settings)

    while (startingPointWithId.isDefined) {
      expandCluster(points, partitionIndex, startingPointWithId.get._2, settings)
      startingPointWithId = findUnvisitedCorePoint(points, settings)
    }

    points.map ( pt => (new PointSortKey(pt._2), pt._2.toImmutablePoint)).iterator
  }

  /** Expands a cluster. In contrast to the function described in http://en.wikipedia.org/wiki/DBSCAN ,
    * this function uses precomputed number of point's neighbors to determine whether the point
    * should be added to a cluster
    *
    * @param points All points
    * @param index An indexing data structure
    * @param startingPoint A point from which the expansion should start
    * @param settings Clustering settings
    */
  private def expandCluster (points: Map[TempPointId, PartiallyMutablePoint], index: PartitionIndex,
    startingPoint: PartiallyMutablePoint, settings: DbscanSettings): Unit = {

    val corePointsInCluster = new scala.collection.mutable.BitSet (points.size)

    corePointsInCluster += startingPoint.tempId

    startingPoint.transientClusterId = startingPoint.pointId
    startingPoint.visited = true

    while (!corePointsInCluster.isEmpty) {
      val currentPointId = corePointsInCluster.head
      val neighbors = findUnvisitedNeighbors(index, points (currentPointId), settings)

      neighbors.foreach {
        n =>
          n.visited = true

          if (n.precomputedNumberOfNeighbors >= settings.numberOfPoints) {
            n.transientClusterId = startingPoint.transientClusterId
            corePointsInCluster += n.tempId
          }
          else if (!settings.treatBorderPointsAsNoise) {
            n.transientClusterId = startingPoint.transientClusterId
          }
          else {
            n.transientClusterId = DbscanModel.NoisePoint
          }
      }

      corePointsInCluster -= currentPointId
    }
  }


  /** Finds point's neighbors which haven't been visited by the clustering algorithm yet
   *
   * @param index An indexing data structure
   * @param pt A point whose neighbors should be found
   * @param settings Clustering settings
   * @return A collection of point's neighbors which haven't been visited yet
   */
  private def findUnvisitedNeighbors (index: PartitionIndex, pt: PartiallyMutablePoint, settings: DbscanSettings)
    :Iterable[PartiallyMutablePoint] = {

    index
      .findClosePoints(pt)
      .map ( p => p.asInstanceOf[PartiallyMutablePoint] )
      .filter {
        p => {
          !p.visited &&
          p.transientClusterId == DbscanModel.UndefinedCluster &&
          calculateDistance(p, pt) <= settings.epsilon
        }
      }
  }


  /** Finds unvisited point which has enough neighbors to be considered a core point of a cluster
    *
    * @param points All points
    * @param settings Clustering settings
    * @return A point which has enough neighbors to be considered a core point of a cluster
    */
  private def findUnvisitedCorePoint (points: Map[TempPointId, PartiallyMutablePoint], settings: DbscanSettings)
    :Option[(TempPointId, PartiallyMutablePoint)] = {

    points.find( pt => !pt._2.visited && pt._2.precomputedNumberOfNeighbors >= settings.numberOfPoints)
  }


  /** Merges clusters found in different partitions of a dataset
    *
    * At the moment when this method is called, all partitions of the dataset are processed. Each point in each partition
    * is assigned a cluster ID, but these IDs are different in each partition. The goal of this method is to detect
    * clusters which spread across different partitions and to assign cluster IDs properly
    *
    * @param partiallyClusteredData An RDD of points along with their sort keys. Each point is assigned a cluster ID but
    *                               these IDs are different in each partition
    * @param boxes A collection of boxes which represent partitions of the dataset
    * @return An RDD of points with properly assigned cluster IDs
    */
  private def mergeClustersFromDifferentPartitions (partiallyClusteredData: RDD[(PointSortKey, Point)],
    boxes: Iterable[Box]): RDD[Point] = {

    val distanceAnalyzer = new DistanceAnalyzer (settings)
    val pointsCloseToBoxBounds = distanceAnalyzer.findPointsCloseToBoxBounds(partiallyClusteredData, boxes,
      settings.epsilon)

    DebugHelper.doAndSaveResult(partiallyClusteredData.sparkContext, "cpdb") {
      path => {
        pointsCloseToBoxBounds.map ( x =>  x.coordinates.mkString(",") + "," + x.clusterId ).saveAsTextFile(path)
      }
    }

    val (mappings, borderPoints) = generateMappings (pointsCloseToBoxBounds, boxes)

    val broadcastMappings = partiallyClusteredData.sparkContext.broadcast(mappings)
    val broadcastBorderPoints = partiallyClusteredData.sparkContext.broadcast(borderPoints)


    DebugHelper.doAndSaveResult (partiallyClusteredData.sparkContext, "mappings") {
      path => {
        val mappingsAsStrings = mappings.toArray.map ( _.toString )
        partiallyClusteredData.sparkContext.parallelize(mappingsAsStrings).saveAsTextFile(path)
      }
    }

    partiallyClusteredData.mapPartitions {
      it => {
        val m = broadcastMappings.value
        val bp = broadcastBorderPoints.value

        it.map ( x => reassignClusterId(x._2, m, bp) )
      }
    }
  }

  private def generateMappings (pointsCloseToBoxBounds: RDD[Point], boxes: Iterable[Box])
    :(HashSet[(HashSet[ClusterId], ClusterId)], Map [PointId, ClusterId])  = {

    val pointsInAdjacentBoxes = PointsInAdjacentBoxesRDD (pointsCloseToBoxBounds, boxes)

    val pairwiseMappings: RDD[(ClusterId, ClusterId)] = pointsInAdjacentBoxes.mapPartitionsWithIndex {
      (idx, it) => {
        val pointsInPartition = it.map(_._2).toArray.sortBy(_.distanceFromOrigin)
        val pairs = HashSet[(ClusterId, ClusterId)] ()

        for (i <- 1 until pointsInPartition.length) {
          var j = i-1

          val pi = pointsInPartition(i)

          while (j >= 0 && pi.distanceFromOrigin - pointsInPartition(j).distanceFromOrigin <= settings.epsilon) {

            val pj = pointsInPartition(j)

            if (pi.boxId != pj.boxId && pi.clusterId != pj.clusterId && calculateDistance(pi, pj) <= settings.epsilon) {

              val enoughCorePoints = if (settings.treatBorderPointsAsNoise) {
                isCorePoint(pi, settings) && isCorePoint (pj, settings)
              }
              else {
                isCorePoint (pi, settings) || isCorePoint (pj, settings)
              }

              if (enoughCorePoints) {

                val (c1, c2) = addBorderPointToCluster(pi, pj, settings)


                if (c1 != c2) {
                  if (pi.clusterId < pj.clusterId) {
                    pairs += ((pi.clusterId, pj.clusterId))
                  }
                  else {
                    pairs += ((pj.clusterId, pi.clusterId))
                  }
                }
              }
            }

            j -= 1
          }
        }

        pairs.iterator
      }
    }

    val borderPointsToBeAssignedToClusters = if (!settings.treatBorderPointsAsNoise) {
      pointsInAdjacentBoxes.mapPartitionsWithIndex {
        (idx, it) => {
          val pointsInPartition = it.map(_._2).toArray.sortBy(_.distanceFromOrigin)
          val bp = scala.collection.mutable.Map[PointId, ClusterId]()

          for (i <- 1 until pointsInPartition.length) {
            var j = i-1
            val pi = pointsInPartition(i)


            while (j >= 0 && pi.distanceFromOrigin - pointsInPartition(j).distanceFromOrigin <= settings.epsilon) {

              val pj = pointsInPartition(j)

              if (pi.boxId != pj.boxId && pi.clusterId != pj.clusterId && calculateDistance(pi, pj) <= settings.epsilon) {
                val enoughCorePoints = isCorePoint(pi, settings) || isCorePoint(pj, settings)

                if (enoughCorePoints) {
                  addBorderPointToCluster(pi, pj, settings, bp)
                }
              }

              j -= 1
            }
          }

          bp.iterator
        }
      }.collect().toMap
    }
    else {
      HashMap[PointId, ClusterId] ()
    }

    val mappings = HashSet[HashSet[ClusterId]] ()
    val processedPairs = HashSet[(ClusterId, ClusterId)] ()

    val temp = pairwiseMappings.collect ()

    temp.foreach {
      x => {
        processPairOfClosePoints(null, null, x._1, x._2, processedPairs, mappings)
      }
    }

    val finalMappings = mappings.filter(_.size > 0).map ( x => (x, x.head) )

    (finalMappings, borderPointsToBeAssignedToClusters)
  }

  private def addBorderPointToCluster (pt1: Point,
                                       pt2: Point,
                                       settings: DbscanSettings): (ClusterId, ClusterId) = {

    var newClusterId1 = pt1.clusterId
    var newClusterId2 = pt2.clusterId

    if (!settings.treatBorderPointsAsNoise) {
      if (!isCorePoint(pt1, settings) && pt1.clusterId == DbscanModel.UndefinedCluster) {
        newClusterId1 = pt2.clusterId
      }
      else if (!isCorePoint (pt2, settings) && pt2.clusterId == DbscanModel.UndefinedCluster) {
        newClusterId2 = pt1.clusterId
      }
    }

    (newClusterId1, newClusterId2)
  }

  /** Detects clusters in different partitions which should be merged together and generates mappings
    * between cluster IDs. Also detects border points which should be assigned to clusters (some of these points
    * are not detected by the findClustersInOnePartition method)
    *
    * @param localData A collection of points which lie close to the bounds of a partition of a dataset
    * @return A tuple with the following structure:
    *         The first element is a set where each element is a tuple. The 1st element of this tuple is a collection of
    *         cluster IDs which represent the same cluster. The 2nd element is an ID of this cluster
    *         The 2nd element of the tuple is a map of points not detected by findClustersInOnePartition method
    */
  private [dbscan] def generateMappings (localData: Seq[Point]): (HashSet[(HashSet[ClusterId], ClusterId)],
    scala.collection.mutable.Map [PointId, ClusterId]) = {

    val processedPairs = HashSet [(ClusterId, ClusterId)] ()
    val mappings = HashSet[HashSet[ClusterId]] ()
    val borderPointsToBeAssignedToClusters = scala.collection.mutable.Map [PointId, ClusterId] ()
    val numPoints = localData.size
    var innerLoopIterations = 0

    for (i <- 1 until numPoints) {

      var j = i-1

      while (j >= 0 && localData(i).distanceFromOrigin - localData(j).distanceFromOrigin <= settings.epsilon) {

        val pt1 = localData(i)
        val pt2 = localData(j)

        if (pt1.boxId != pt2.boxId) {

          val dist = calculateDistance(pt1, pt2)

          if (dist <= settings.epsilon) {
            val enoughCorePoints = if (settings.treatBorderPointsAsNoise) {
              isCorePoint(pt1, settings) && isCorePoint (pt2, settings)
            }
            else {
              isCorePoint (pt1, settings) || isCorePoint (pt2, settings)
            }

            if (enoughCorePoints) {

              val (c1, c2) = addBorderPointToCluster(pt1, pt2, settings, borderPointsToBeAssignedToClusters)

              if (c1 != c2) {
                processPairOfClosePoints(pt1, pt2, c1, c2, processedPairs, mappings)
              }
            }
          }
        }

        j -= 1
        innerLoopIterations += 1
      }
    }

    val finalMappings = mappings.map ( x => (x, x.head) )

    logInfo (s"Number of points: $numPoints ; iterations: $innerLoopIterations")

    (finalMappings, borderPointsToBeAssignedToClusters)
  }


  private def processPairOfClosePoints (pt1: Point,
                                        pt2: Point,
                                        c1: ClusterId,
                                        c2: ClusterId,
                                        processedPairs: HashSet[(ClusterId, ClusterId)],
                                        mappings: HashSet[HashSet[ClusterId]]): Unit = {

    val pair = if (c1 < c2) {
      (c1, c2)
    }
    else {
      (c2, c1)
    }

    if (!processedPairs.contains(pair)) {

      processedPairs += pair

      val m1 = mappings.find ( _.contains(c1))
      val m2 = mappings.find ( _.contains(c2))

      (m1, m2) match {
        case (None, None) => mappings += HashSet (c1, c2)
        case (None, y: Some[HashSet[ClusterId]]) => y.get += c1
        case (x: Some[HashSet[ClusterId]], None) => x.get += c2
        case (x: Some[HashSet[ClusterId]], y: Some[HashSet[ClusterId]]) => {

          if (x != y) {
            mappings += x.get.union(y.get)

            // why mappings -= x.get doesn't work???????
            x.get.clear()
            y.get.clear()
          }
        }
      }
    }
  }




  private def addBorderPointToCluster (
    pt1: Point,
    pt2: Point,
    settings: DbscanSettings,
    borderPointsToBeAssignedToClusters: scala.collection.mutable.Map [PointId, ClusterId]): (ClusterId, ClusterId) = {

    var newClusterId1 = pt1.clusterId
    var newClusterId2 = pt2.clusterId

    if (!settings.treatBorderPointsAsNoise) {
      if (!isCorePoint(pt1, settings) && pt1.clusterId == DbscanModel.UndefinedCluster) {
        borderPointsToBeAssignedToClusters.put(pt1.pointId, pt2.clusterId)
        newClusterId1 = pt2.clusterId
      }
      else if (!isCorePoint (pt2, settings) && pt2.clusterId == DbscanModel.UndefinedCluster) {
        borderPointsToBeAssignedToClusters.put (pt2.pointId, pt1.clusterId)
        newClusterId2 = pt1.clusterId
      }
    }

    (newClusterId1, newClusterId2)
  }


  private [dbscan] def reassignClusterId (
    pt: Point,
    mappings: HashSet[(HashSet[ClusterId], ClusterId)],
    borderPoints: Map [PointId, ClusterId]): Point = {

    var newClusterId = pt.clusterId

    if (borderPoints.contains(pt.pointId)) {

      // It is a border point which belongs to a cluster whose core points reside in a different box
      newClusterId = borderPoints (pt.pointId)
    }

    val mapping = mappings.find ( _._1.contains(newClusterId) )

    mapping match {
      case m: Some[(HashSet[ClusterId], ClusterId)] => newClusterId = m.get._2
      case _ =>
    }

    if (newClusterId == DbscanModel.UndefinedCluster) {

      // If all attempts to find a suitable cluster id for a point failed,
      // then we considering this point as noise
      newClusterId = DbscanModel.NoisePoint
    }

    pt.withClusterId(newClusterId)
  }


  private def isCorePoint (pt: Point, settings: DbscanSettings): Boolean = {
    pt.precomputedNumberOfNeighbors >= settings.numberOfPoints
  }
}
