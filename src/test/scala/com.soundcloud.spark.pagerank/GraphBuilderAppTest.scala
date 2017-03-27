package com.soundcloud.spark.pagerank

import org.scalatest.{ FunSuite, Matchers }

class GraphBuilderAppTest
  extends FunSuite
  with Matchers
  with GraphTesting
  with SparkTesting {

  // TODO(jd): design a better integration test as this just runs the app without assertions
  test("integration test") {
    val options = new GraphBuilderApp.Options()
    options.output = "target/test/GraphBuilderAppTest"

    val input = sc.parallelize(Seq(
      (1, 5, 1.0),
      (2, 1, 1.0),
      (3, 1, 1.0),
      (4, 2, 1.0),
      (4, 3, 1.0),
      (5, 3, 1.0),
      (5, 4, 1.0)
    ).map(_.productIterator.toSeq.mkString("\t")))

    GraphBuilderApp.runFromInputs(options, sc, input)
  }
}
