package com.soundcloud.spark.pagerank

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Functions for building graphs or parts of graphs from more basic components,
 * for the purposes of testing only.
 */
trait GraphTesting {

  type EdgeTuple = (Int, Int, Value)

  def sc: SparkContext

  implicit def tupleToEdge(t: EdgeTuple): Edge = {
    val (srcId, dstId, weight) = t
    Edge(srcId, dstId, weight)
  }

  implicit def edgeToTuple(edge: Edge): EdgeTuple =
    (edge.srcId.toInt, edge.dstId.toInt, edge.weight)

  implicit def edgeSeqToTupleSeq(edges: Seq[Edge]): Seq[EdgeTuple] =
    edges.map(edgeToTuple)

  implicit def tupleSeqToEdgeSeq(tuples: Seq[EdgeTuple]): Seq[Edge] =
    tuples.map(tupleToEdge)

  implicit def edgeSeqToRDD(edges: Seq[Edge]): EdgeRDD =
    sc.parallelize(edges)

  implicit def tupleSeqToRDD(tuples: Seq[EdgeTuple]): EdgeRDD =
    edgeSeqToRDD(tupleSeqToEdgeSeq(tuples))

  implicit def vertexSeqToRDD(vertices: Seq[Vertex]): VertexRDD =
    sc.parallelize(vertices)
}
