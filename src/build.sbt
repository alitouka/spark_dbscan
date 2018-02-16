name := "spark_dbscan"

organization := "org.alitouka"

version := "0.0.4"

scalaVersion := "2.10.6"

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.1" % "provided"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "3.0.4" % Test

libraryDependencies += "org.apache.commons" % "commons-math3" % "3.2"

libraryDependencies += "com.github.scopt" %% "scopt" % "3.7.0"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

test in assembly := {}