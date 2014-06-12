Spark DBSCAN is implementation of the [DBSCAN clustering algorithm](http://en.wikipedia.org/wiki/DBSCAN) on top of [Apache Spark](http://spark.apache.org/) . It also includes 2 simple tools which will help you choose parameters of the DBSCAN algorithm.

![Clusters identified by the DBSCAN algorithm](https://github.com/alitouka/spark_dbscan/raw/master/wiki/img/finally_clustered.png)

This software is **EXPERIMENTAL** , it supports only Euclidean and Manhattan distance measures ( [why?](How-It-Works#why-not-all-distance-measures-are-supported) ) and it is not well optimized yet. I tested it only on small datasets (millions of points) in 2-dimensional space.

You can use Spark DBSCAN as a standalone application which you can submit to a Spark cluster ( [Learn how](Using-Spark-DBSCAN-from-the-command-line) ). Alternatively, you can include it into your own app - its API is documented and easy to use ( [Learn how](../Including-Spark-DBSCAN-into-your-application) ).

Learn more about:

* [How it works](How-It-Works)
* [How to use it as a command-line tool](Using-Spark-DBSCAN-from-the-command-line)
* [How to choose parameters of DBSCAN algorithm](Choosing-parameters-of-DBSCAN-algorithm)
* [How to include it into your application](Including-Spark-DBSCAN-into-your-application)

## Performance
TODO: insert chart
