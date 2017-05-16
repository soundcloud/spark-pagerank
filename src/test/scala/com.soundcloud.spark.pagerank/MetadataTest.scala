package com.soundcloud.spark.pagerank

import java.io.File

import org.apache.commons.io.FileUtils
import org.scalatest.{ BeforeAndAfter, FunSuite, Matchers }

class MetadataTest
  extends FunSuite
  with BeforeAndAfter
  with Matchers
  with SparkTesting {

  val path = "target/test/MetadataTest"
  val metadata = Metadata(numVertices=1)

  before {
    FileUtils.deleteDirectory(new File(path))
  }

  test("save and load") {
    Metadata.save(spark, metadata, path)
    Metadata.load(spark, path) shouldBe (metadata)
  }
}
