package com.soundcloud.spark.pagerank

import java.io.File

import org.apache.commons.io.FileUtils
import org.scalatest.{ BeforeAndAfter, FunSuite, Matchers }

class GraphBuilderAppTest
  extends FunSuite
  with BeforeAndAfter
  with Matchers
  with GraphTesting
  with SparkTesting {

  val path = "target/test/GraphBuilderAppTest"

  before {
    FileUtils.deleteDirectory(new File(path))
  }

  // TODO(jd): design a better integration test as this just runs the app without assertions
  test("integration test") {
    val options = new GraphBuilderApp.Options()
    options.output = path
    options.numPartitions = 1

    val input = spark.sparkContext.parallelize(Seq(
      (1, 5, 1.0),
      (2, 1, 1.0),
      (3, 1, 1.0),
      (4, 2, 1.0),
      (4, 3, 1.0),
      (5, 3, 1.0),
      (5, 4, 1.0)
    ).map(_.productIterator.toSeq.mkString("\t")))

    GraphBuilderApp.runFromInputs(options, spark, input)
  }
}
