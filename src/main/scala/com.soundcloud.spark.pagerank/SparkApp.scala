package com.soundcloud.spark.pagerank

import org.apache.spark.{ SparkConf, SparkContext }

/**
 * Common trait for bootstrapping a Spark app.
 */
trait SparkApp {
  def run(args: Array[String], sc: SparkContext): Unit

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf())
    run(args, sc)
    sc.stop()
  }
}
