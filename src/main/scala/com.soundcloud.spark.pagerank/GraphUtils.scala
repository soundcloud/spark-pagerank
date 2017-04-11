package com.soundcloud.spark.pagerank

import scala.collection.mutable.ListBuffer

import org.apache.spark.rdd.RDD

/**
 * Some general purpose graph operations and utilities. Any operations specific
 * to the complete PageRank graph will be in there and not here.
 */
object GraphUtils {

  /**
   * Given the edges of a graph, this unzips the source and destination vertex
   * IDs. ID's in the resulting RDDs are distinct.
   */
  def unzipDistinct(edges: EdgeRDD): (RDD[Id], RDD[Id]) = {
    def extract(fn: (Edge) => Id): RDD[Id] =
      edges.map(fn).distinct

    (extract(_.srcId), extract(_.dstId))
  }

  /**
   * Given an RDD of source vertex IDs and an RDD of destination vertex IDs
   * (from edges), tag vertex IDs with a flag to indicate if the vertex is
   * dangling (no out edges) or not.
   */
  def tagDanglingVertices(srcIds: RDD[Id], dstIds: RDD[Id]): RDD[(Id, Boolean)] = {
    // determine which vertices are dangling
    val srcIdPairs = srcIds.map(x => (x, 1))
    val dstIdPairs = dstIds.map(x => (x, 1))

    srcIdPairs
      .cogroup(dstIdPairs)
      .map { case (id, vals) =>
        val isDangling = (vals._1.size == 0 && vals._2.size == 1)
        (id, isDangling)
      }
  }

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
    val (srcIds, dstIds) = unzipDistinct(edges)
    tagDanglingVertices(srcIds, dstIds).filter(_._2).count()
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
  def areVerticesNormalized[T](vertices: VertexRDD, eps: Value = EPS): Boolean =
    math.abs(1.0 - vertices.map(_.value).sum()) <= eps

  /**
   * Counts the number of vertices that do not have edges that sum to 1.0.
   * Assumes edges with `Double` weights.
   */
  def countVerticesWithoutNormalizedOutEdges(edges: EdgeRDD, eps: Value = EPS): Long = {
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

  /**
   * Validates the structure of the input PageRank graph, according to the
   * requirements to run PageRank. Returns a list of validation errors, if any.
   *
   * Performance note: `edges` are iterated over three times, and `vertices`
   * once, so please consider persisting either or both before running this.
   */
  def validateStructure(edges: EdgeRDD, vertices: VertexRDD, eps: Value = EPS): Option[Seq[String]] = {
    val errors = ListBuffer.empty[String]

    val numSelfReferences = countSelfReferences(edges)
    if (numSelfReferences != 0)
      errors.append(s"Number of vertices with self-referencing edges must be 0, but was $numSelfReferences")

    if (!areVerticesNormalized(vertices, eps))
      errors.append("Input vertices values must be normalized")

    val numVerticesWithoutNormalizedOutEdges = countVerticesWithoutNormalizedOutEdges(edges)
    if (numVerticesWithoutNormalizedOutEdges != 0)
      errors.append(s"Number of vertices without normalized out edges must be 0, but was $numVerticesWithoutNormalizedOutEdges")

    if (errors.isEmpty)
      None
    else
      Some(errors.toSeq)
  }
}
