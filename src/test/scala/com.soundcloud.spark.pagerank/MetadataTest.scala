package com.soundcloud.spark.pagerank

import org.scalatest.{ Matchers, FunSuite }

class MetadataTest
  extends FunSuite
  with Matchers
  with SparkTesting {

  val stats = Seq(
    ("integer", "1"),
    ("long", "1"),
    ("string", "1"),
    ("double", "1.0")
  )

  test("save and load") {
    val path ="target/test/MetadataTest"
    Metadata.save(spark, stats, path)
    execTestExtractStatistic(Metadata.load(spark, path))
    Metadata.loadAndExtract(spark, path, "integer")(_.toInt) shouldBe 1
  }

  test("extract statistic") {
    execTestExtractStatistic(stats)
  }

  def execTestExtractStatistic(stats: Seq[(String, String)]): Unit = {
    Metadata.extract(stats, "integer")(_.toInt) shouldBe 1
    Metadata.extract(stats, "long")(_.toLong) shouldBe 1L
    Metadata.extract(stats, "double")(_.toDouble) shouldBe 1.0
  }
}
