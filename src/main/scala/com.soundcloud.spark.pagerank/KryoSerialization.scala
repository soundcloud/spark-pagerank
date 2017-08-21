package com.soundcloud.spark.pagerank

import org.apache.spark.SparkConf

/**
 * Trait for using Kryo serialization. Extend your drivers with this trait and
 * simply call [[useKryo]].
 */
trait KryoSerialization {

  /**
   * Registers internal case classes for Kryo serialization.
   */
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
