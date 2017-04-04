package com.soundcloud.spark.pagerank

import org.apache.spark.SparkConf

/**
 * Trait for using Kryo serialization.
 */
trait KryoSerialization {

  def useKryo(conf: SparkConf): Unit = {
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(
      classOf[Edge],
      classOf[Vertex],
      classOf[OutEdge],
      classOf[VertexMetadata]
    ))
  }
}
