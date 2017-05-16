package com.soundcloud.spark.pagerank

import org.apache.spark.sql.SparkSession

/**
 * A representation of the superset of metadata about a PageRank graph.
 */
case class Metadata(numVertices: Long)

/**
 * A very basic way to manage typed metadata as JSON on disk. This allows for
 * simple human inspection and easy reading/writing via Datasets.
 */
object Metadata {
  def save(spark: SparkSession, metadata: Metadata, path: String): Unit = {
    import spark.implicits._
    Seq(metadata).toDS().write.json(path)
  }

  def load(spark: SparkSession, path: String): Metadata = {
    import spark.implicits._
    spark.read.json(path).as[Metadata].head()
  }
}
