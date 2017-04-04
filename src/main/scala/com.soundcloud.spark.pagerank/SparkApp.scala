package com.soundcloud.spark.pagerank

import org.apache.spark.{ SparkConf, SparkContext }

/**
 * Common trait for bootstrapping a Spark app.
 */
trait SparkApp extends KryoSerialization {
  def run(args: Array[String], sc: SparkContext): Unit

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    useKryo(conf)

    val sc = new SparkContext(conf)
    setupLogging(sc)

    run(args, sc)
    sc.stop()
  }

  def setupLogging(sc: SparkContext): Unit =
    sc.setLogLevel("WARN")
}
