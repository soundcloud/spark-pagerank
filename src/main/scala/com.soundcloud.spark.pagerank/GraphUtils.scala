package com.soundcloud.spark.pagerank

import org.apache.spark.rdd.RDD

/**
 * Some general purpose graph operations and utilities.
 */
object GraphUtils {

  /**
   * Counts the number of vertices with no out edges. These are considered as
   * "dangling" vertices.
   *
   * This is implemented by a set operation, where the dangling vertices are
   * those that appear in the set of destination vertex IDs only (and not in
   * the source vertex IDs).
   *
   * Performance note: `edges` are iterated over twice, so please consider
   * persisting it first.
   */
  def countDanglingVertices(edges: EdgeRDD): Long = {
    val a = edges.map(_.srcId).distinct.map(x => (x, 1))
    val b = edges.map(_.dstId).distinct.map(x => (x, 1))

    a
      .cogroup(b)
      .filter { case (_, vals) =>
        (vals._1.size == 0 && vals._2.size == 1)
      }.
      count()
  }

  /**
   * Counts the number of vertices that have self-referencing edges.
   */
  def countSelfReferences(edges: EdgeRDD): Long = {
    edges
      .filter(_.isSelfReferencing)
      .map(_.srcId)
      .distinct()
      .count()
  }

  /**
   * Determines if the vertices of a graph are normalized. Assumes a graph with
   * `Double` vertex attributes.
   */
  def areVerticesNormalized[T](vertices: VertexRDD, eps: Double = EPS): Boolean =
    math.abs(1.0 - vertices.map(_.value).sum()) <= eps

  /**
   * Counts the number of vertices that do not have edges that sum to 1.0.
   * Assumes edges with `Double` weights.
   */
  def countVerticesWithoutNormalizedOutEdges(edges: EdgeRDD, eps: Double = EPS): Long = {
    edges
      .map(edge => (edge.srcId, edge.weight))
      .reduceByKey(_ + _)
      .filter { case (_, weightSum) =>
        math.abs(1.0 - weightSum) > eps
      }
      .count()
  }

  /**
   * Normalizes outgoing edge weights of an {{EdgeRDD}}.
   *
   * Performance note: `edges` are iterated over twice, so please consider
   * persisting it first.
   */
  def normalizeOutEdgeWeights(edges: EdgeRDD): EdgeRDD = {
    val sums = edges
      .map { edge =>
        (edge.srcId, edge.weight)
      }
      .reduceByKey(_ + _)
    edges
      .map(_.toOutEdgePair)
      .join(sums)
      .map { case (srcId, (outEdge, weightSum)) =>
        Edge(srcId, outEdge.dstId, outEdge.weight / weightSum)
      }
  }
}
