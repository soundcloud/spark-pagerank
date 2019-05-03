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
    conf.set("spark.kryo.referenceTracking", "false") // See: https://issues.apache.org/jira/browse/SPARK-21347?focusedCommentId=16176327&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-16176327
    conf.registerKryoClasses(Array(
      classOf[Edge],
      classOf[Vertex],
      classOf[OutEdge],
      classOf[VertexMetadata]
    ))
  }
}
