package com.soundcloud.spark

import scala.reflect.ClassTag

import org.apache.spark.graphx.{ Edge, Graph, VertexId }
import org.apache.spark.rdd.RDD

/**
 * Some general-purpose graph operations and utilities.
 *
 * TODO(jd): move to `spark-lib`
 */
trait GraphUtils {

  val EPS: Double = 1.0E-15 // machine epsilon: http://en.wikipedia.org/wiki/Machine_epsilon

  /**
   * Counts the number of vertices with no out edges. These are considered as
   * "dangling" vertices.
   *
   * This is implemented by a set operation, where the dangling vertices are
   * those that appear in the set of destination vertex IDs only (and not in
   * the source vertex IDs).
   *
   * Note that this iterates over `edges` twice, caching it before calling this
   * method is advised if performance is of concern.
   */
  def countDanglingVertices[ED: ClassTag](edges: RDD[Edge[ED]]): Long = {
    val a = edges.map(_.srcId).distinct.map(x => (x, 1))
    val b = edges.map(_.dstId).distinct.map(x => (x, 1))

    a
      .cogroup(b)
      .filter { case(k, vals) =>
        (vals._1.size == 0 && vals._2.size == 1)
      }.
      count()
  }

  /**
   * Counts the number of vertices that have self-referencing edges.
   */
  def countSelfReferences[ED](edges: RDD[Edge[ED]]): Long =
    edges.filter(e => e.srcId == e.dstId).map(_.srcId).distinct().count()

  /**
   * Counts the number of vertices that do not have edges that sum to 1.0.
   * Assumes edges with `Double` weights.
   */
  def countVerticesWithoutNormalizedOutEdges(edges: RDD[Edge[Double]], eps: Double = EPS): Long = {
    edges
      .map(edge => (edge.srcId, edge.attr))
      .reduceByKey(_ + _)
      .filter { case (k, v) =>
        math.abs(1.0 - v) > eps
      }
      .count()
  }

  /**
   * Determines if the vertices of a graph are normalized. Assumes a graph with
   * `Double` vertex attributes.
   */
  def areVerticesNormalized[T](vertices: RDD[(T, Double)], eps: Double = EPS): Boolean =
    math.abs(1.0 - vertices.map(_._2).sum) <= eps

  /**
   * Normalizes the outgoing edge weights (`Double`) of an RDD of Edges.
   */
  def normalizeOutEdgeWeightsRDD(edges: RDD[Edge[Double]]): RDD[Edge[Double]] = {
    val sums = edges
      .map { edge =>
        (edge.srcId, edge.attr)
      }
      .reduceByKey(_ + _)
    edges
      .map { edge =>
        (edge.srcId, edge)
      }
      .join(sums)
      .map { case(_, (edge, weightSum)) =>
        Edge(edge.srcId, edge.dstId, edge.attr / weightSum)
      }
  }

  /**
   * Removes any edges that are self-referencing the same vertex. That is, any
   * edges where the source and destination are the same. Any resulting vertices
   * that have no edges (in or out) will remain in the graph.
   */
  def removeSelfReferences[VD, ED](graph: Graph[VD, ED]): Graph[VD, ED] =
    graph.subgraph(e => e.srcId != e.dstId)

  /**
   * Removes any vertices that have no in or out edges.
   */
  def removeDisconnectedVertices[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Graph[VD, ED] = {
    graph.filter(
      preprocess = g => g.outerJoinVertices(g.degrees)((_, _, deg) => deg.getOrElse(0)),
      vpred = (_: VertexId, deg: Int) => deg > 0
    )
  }
}
