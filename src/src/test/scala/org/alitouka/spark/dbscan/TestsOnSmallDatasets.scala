package org.alitouka.spark.dbscan


import org.alitouka.spark.dbscan.util.io.IOHelper

class TestsOnSmallDatasets extends DbscanSuiteBase {

//  "Density-based partitioning" should "work" in {
//
//    val partitioningSettings = new PartitioningSettings (numberOfPointsInBox = 10000)
//    val data = readDataset ("/home/cloudera/Activities/Programming_Personal/Spark/temp/Prototypes/dbscan_prototype/test_examples/small_2d/data/9_100K.csv")
//
//    val partitionedData = PartitionedUnsortedDataSet (data, partitioningSettings)
//
//    val dataWithPartitionIndices = partitionedData.data.mapPartitionsWithIndex {
//      (idx, it) => {
//        it.map ( x => x._2.coordinates.mkString(",") + "," + idx )
//      }
//    }
//
//    dataWithPartitionIndices.saveAsTextFile("/home/cloudera/Activities/Programming_Personal/Spark/temp/Prototypes/dbscan_prototype/test_examples/small_2d/data/points")
//
//    val boxBoundaries = partitionedData.boxes.map {
//      box => {
//        Array (box.bounds(0).lower, box.bounds(1).lower, box.bounds(0).higher, box.bounds(1).higher).mkString (",")
//      }
//    }.toArray
//
//    data.sparkContext.parallelize(boxBoundaries).saveAsTextFile("/home/cloudera/Activities/Programming_Personal/Spark/temp/Prototypes/dbscan_prototype//boxes")
//
//  }

//  "DBSCAN algorithm" should "Work with density-based partitioning" in {
//    val settings = new DbscanSettings ().withEpsilon (20).withNumberOfPoints (30)
//    val data = readDataset ("/home/cloudera/Activities/Programming_Personal/Spark/temp/Prototypes/dbscan_prototype/test_examples/small_2d/data/9_100K.csv")
//    val clusteredData = Dbscan.train(data, settings)
//    IOHelper.saveClusteringResult(clusteredData, "/home/cloudera/Activities/Programming_Personal/Spark/temp/Prototypes/dbscan_prototype/out")
//  }

}
