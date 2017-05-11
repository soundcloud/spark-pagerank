package com.soundcloud.spark.pagerank

import org.apache.spark.sql.SparkSession

/**
 * Functions for building graphs or parts of graphs from more basic components,
 * for the purposes of testing only.
 */
trait GraphTesting {

  type EdgeTuple = (Int, Int, Value)

  def spark: SparkSession

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
    spark.sparkContext.parallelize(edges)

  implicit def tupleSeqToRDD(tuples: Seq[EdgeTuple]): EdgeRDD =
    edgeSeqToRDD(tupleSeqToEdgeSeq(tuples))

  implicit def vertexSeqToRDD(vertices: Seq[Vertex]): VertexRDD =
    spark.sparkContext.parallelize(vertices)
}
