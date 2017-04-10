package com.soundcloud.spark

import org.apache.spark.rdd.RDD

package object pagerank {
  type Id = Long
  type Value = Double

  type EdgeRDD = RDD[Edge]
  type VertexRDD = RDD[Vertex]

  type OutEdgePair = (Id, OutEdge)
  type VertexPair = (Id, Value)
  type RichVertexPair = (Id, VertexMetadata)

  type OutEdgePairRDD = RDD[OutEdgePair]
  type RichVertexPairRDD = RDD[RichVertexPair]

  final case class Edge(srcId: Id, dstId: Id, weight: Value) {
    def isSelfReferencing: Boolean = srcId == dstId
    def toOutEdgePair: OutEdgePair = (srcId, OutEdge(dstId, weight))
  }

  final case class Vertex(id: Id, value: Value) {
    def toPair: VertexPair = (id, value)
  }

  final case class OutEdge(dstId: Id, weight: Value)

  final case class VertexMetadata(value: Value, isDangling: Boolean) {
    /**
     * Copies the vertex properties but provides a new value. This creates a new
     * immutable vertex instance.
     */
    def withNewValue(newValue: Value): VertexMetadata =
      VertexMetadata(newValue, isDangling)
  }

  val EPS: Value = 1.0E-15 // machine epsilon: http://en.wikipedia.org/wiki/Machine_epsilon
}
