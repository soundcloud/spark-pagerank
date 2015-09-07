package com.soundcloud.spark

import scala.reflect.ClassTag

import org.apache.spark.graphx.{ EdgeTriplet, Graph, VertexId }

/**
 * Some general-purpose graph operations and utlities.
 *
 * TODO: move to `spark-lib`
 */
trait GraphUtils {

  /**
   * Normalizes the outgoing edge weights (`Double`) of a graph.
   */
  def normalizeOutEdgeWeights[V: ClassTag](graph: Graph[V, Double]): Graph[V, Double] = {
    val outgoingWeightSums = graph.aggregateMessages[Double](
      ctx => ctx.sendToSrc(ctx.attr),
      _ + _
    )
    graph.
      outerJoinVertices(outgoingWeightSums) { (vId, vAttr, weightSum) => (vAttr, weightSum.getOrElse(0.0)) }.
      mapTriplets[Double] { e: EdgeTriplet[(V, Double), Double] => e.attr / e.srcAttr._2 }.
      mapVertices { case (_, (vAttr, _)) => vAttr }
  }

  /**
   * Removes any edges that are self-referencing the same vertex. That is, any
   * edges where the source and destination are the same. Any resulting vertices
   * that have no edges (in or out) will remain in the graph.
   */
  def removeSelfReferences[VD, ED](graph: Graph[VD, ED]): Graph[VD, ED] =
    graph.subgraph(e => e.srcId != e.dstId)

  /**
   * Removes any vertices that have no in our out edges.
   */
  def removeDisconnectedVertices[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Graph[VD, ED] = {
    graph.filter(
      preprocess = g => g.outerJoinVertices(g.degrees)((_, _, deg) => deg.getOrElse(0)),
      vpred = (_: VertexId, deg: Int) => deg > 0
    )
  }
}
