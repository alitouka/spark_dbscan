package org.alitouka.spark.dbscan


import org.alitouka.spark.dbscan.util.commandLine.{CommonArgs, CommonArgsParser}
import org.apache.commons.math3.ml.distance.{ManhattanDistance, EuclideanDistance}

class CommandLineParsingSuite extends DbscanSuiteBase {

  val masterUrl = "spark://localhost:7777"
  val jar = "hdfs://somewhere/dbscan_prototype.jar"
  val inputPath = "hdfs://somewhere/in"
  val outputPath = "hdfs://somewhere/out"
  val eps = 0.1
  val minPts = 3
  val customDistanceMeasureClassName = "org.apache.commons.math3.ml.distance.ManhattanDistance"
  val numBuckets = 100

  val requiredArgs = Array ("--ds-master", masterUrl, "--ds-jar", jar, "--ds-input", inputPath, "--ds-output", outputPath)
  val dbscanAdditionalArgs = Array ("--eps", eps.toString, "--numPts", minPts.toString)
  val distanceMeasureArg = Array ("--distanceMeasure", customDistanceMeasureClassName)
  val borderPointsAsNoiseArg = Array ("--borderPointsAsNoise", "1")
  val numberOfBucketsArg = Array ("--numBuckets", numBuckets.toString)


  test ("DbscanDriver.OptionParser should find all required arguments") {
    val parser = new DbscanDriver.ArgsParser ()

    val args = requiredArgs ++ dbscanAdditionalArgs

    val parsingResult = parser.parse(args)

    assert (parsingResult)
    assertThatDbscanRequiredArgumentsWereFound(parser)

    parser.args.distanceMeasure shouldBe a [EuclideanDistance]
    parser.args.borderPointsAsNoise should equal (DbscanSettings.getDefaultTreatmentOfBorderPoints)
  }

  test("DbscanDriver.OptionParser should recognize custom distance measure and treatment of border points") {

    val parser = new DbscanDriver.ArgsParser ()

    val args = requiredArgs ++ dbscanAdditionalArgs ++ distanceMeasureArg ++ borderPointsAsNoiseArg

    val parsingResult = parser.parse(args)

    assert (parsingResult)
    assertThatDbscanRequiredArgumentsWereFound(parser)

    parser.args.distanceMeasure shouldBe a [ManhattanDistance]
    assert (parser.args.borderPointsAsNoise)
  }

  def assertThatDbscanRequiredArgumentsWereFound (parser: DbscanDriver.ArgsParser) {
    parser.args.eps should equal (eps)
    parser.args.minPts should equal (minPts)

    assertThatCommonRequiredArgumentsWereFound(parser)
  }

  def assertThatCommonRequiredArgumentsWereFound [C <: CommonArgs] (parser: CommonArgsParser[C]) {
    parser.args.masterUrl should equal (masterUrl)
    parser.args.jar should equal (jar)
    parser.args.inputPath should equal (inputPath)
    parser.args.outputPath should equal (outputPath)
  }

}
