package org.alitouka.spark.dbscan.spatial.rdd

import org.alitouka.spark.dbscan.SuiteBase
import org.alitouka.spark.dbscan.spatial.Box


class BoxPartitionerSuite extends SuiteBase {

  test ("BoxPartitioner properly convert box ids to partition ids and vice versa") {
    val boxes = Array (new Box ((0.0, 1.0), (0.0, 1.0)).withId(10),
      new Box ((1.0, 2.0), (0.0, 0.1)).withId(20),
      new Box ((2.0, 3.0), (0.0, 0.1)).withId(30))

    val boxesWithPartitionIds = BoxPartitioner.assignPartitionIdsToBoxes(boxes)

    val partitioner = new BoxPartitioner(boxesWithPartitionIds)

    partitioner.numPartitions should be (3)

    partitioner.getPartition(10) should be (0)
    partitioner.getPartition(20) should be (1)
    partitioner.getPartition(30) should be (2)

  }

}
