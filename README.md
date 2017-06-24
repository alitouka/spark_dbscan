Spark DBSCAN is an implementation of the [DBSCAN clustering algorithm](http://en.wikipedia.org/wiki/DBSCAN) on top of [Apache Spark](http://spark.apache.org/) . It also includes 2 simple tools which will help you choose parameters of the DBSCAN algorithm.

![Clusters identified by the DBSCAN algorithm](https://github.com/alitouka/spark_dbscan/raw/master/wiki/img/finally_clustered.png)

This software is **EXPERIMENTAL** , it supports only Euclidean and Manhattan distance measures ( [why?](../../wiki/How-It-Works#why-not-all-distance-measures-are-supported) ) and it is not well optimized yet. I tested it only on small datasets (millions of records with 2 features in each record).

You can use Spark DBSCAN as a standalone application which you can submit to a Spark cluster ( [Learn how](../../wiki/Using-Spark-DBSCAN-as-a-standalone-application) ). Alternatively, you can include it into your own app - its API is [documented](http://alitouka-public.s3-website-us-east-1.amazonaws.com/spark_dbscan/releases/0.0.2/scaladoc/#org.alitouka.spark.dbscan.package) and easy to use ( [Learn how](../../wiki/Including-Spark-DBSCAN-in-your-application) ).

Learn more about:

* [How it works](../../wiki/How-It-Works)
* [How to use it as a standalone application](../../wiki/Using-Spark-DBSCAN-as-a-standalone-application)
* [How to choose parameters of DBSCAN algorithm](../../wiki/Choosing-parameters-of-DBSCAN-algorithm)
* [How to include it in your application](../../wiki/Including-Spark-DBSCAN-in-your-application)

## Performance
![Performance chart](https://github.com/alitouka/spark_dbscan/raw/master/wiki/img/performance_chart_0_0_2.png)

## Credits
 I was glad to receive contributions from other people and I'd like to say thank you:
 * Mark Geraty - for fixing a bug with Java RDDs;
 * [@agrinh](https://github.com/agrinh) - for adding compatibility with Spark 1.1.0
