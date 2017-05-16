package com.soundcloud.spark.pagerank

import org.apache.spark.sql.SparkSession
import org.scalatest.{ BeforeAndAfterAll, Suite }

trait SparkTesting extends BeforeAndAfterAll { self: Suite =>
  var spark: SparkSession = _

  override def beforeAll() {
    self.spark = SparkSession
      .builder
      .appName("test")
      .master("local")
      .config("spark.hadoop.validateOutputSpecs", "false")
      .config("spark.sql.warehouse.dir", "target/test/spark/warehouse")
      .getOrCreate()

    spark.sparkContext.setCheckpointDir("target/test/spark/checkpoint")

    super.beforeAll()
  }

  override def afterAll() {
    if (spark != null)
      spark.stop()

    super.afterAll()
  }
}
