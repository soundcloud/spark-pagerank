package com.soundcloud.spark.pagerank

import java.util.Properties

import org.scalatest.{ BeforeAndAfterAll, Suite }
import org.apache.spark.{ SparkConf, SparkContext }

trait SparkTesting extends BeforeAndAfterAll { self: Suite =>
  var sc: SparkContext = _

  override def beforeAll() {
    val conf = new SparkConf()
      .setAppName("test")
      .setMaster("local")
      .set("spark.hadoop.validateOutputSpecs", "false")
    sc = new SparkContext(conf)
    sc.setCheckpointDir("target/test/checkpoints")

    super.beforeAll()
  }

  override def afterAll() {
    if (sc != null) sc.stop()

    super.afterAll()
  }
}
