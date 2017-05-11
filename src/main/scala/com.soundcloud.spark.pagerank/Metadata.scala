package com.soundcloud.spark.pagerank

import org.apache.spark.sql.SparkSession

/**
 * A very basic way to manage untyped metadata as plain text on disk. This
 * allows for simple human inspection in a TSV and we don't need JSON or other.
 */
object Metadata {
  def save(spark: SparkSession, metadata: Seq[(String, Any)], path: String): Unit = {
    val metaStrs = metadata.map { case (k, v) => s"$k,${v.toString}" }
    spark
      .sparkContext
      .parallelize(metaStrs)
      .repartition(1)
      .saveAsTextFile(path)
  }

  def load(spark: SparkSession, path: String): Seq[(String, String)] = {
    spark.sparkContext.textFile(path).collect().map { x =>
      val Array(k, v) = x.split(",")
      (k, v)
    }
  }

  def extract[T](stats: Seq[(String, String)], key: String)(parse: (String) => T): T = {
    stats.find(_._1 == key) match {
      case Some(pair) => parse(pair._2)
      case None => throw new IllegalArgumentException(s"Statistic not found with key: $key")
    }
  }

  def loadAndExtract[T](spark: SparkSession, path: String, key: String)(parse: (String) => T): T =
    extract(load(spark, path), key)(parse)
}
