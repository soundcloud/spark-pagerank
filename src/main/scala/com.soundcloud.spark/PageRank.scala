package com.soundcloud.spark

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object PageRank {
  type VertexValue = Double
  type EdgeWeight = Double
  type PageRankGraph = Graph[VertexValue, EdgeWeight]
  type VectorRDD = VertexRDD[VertexValue]

  case class VertexMetadata(value: VertexValue, hasOutgoingEdges: Boolean, oldValue: VertexValue = 0.0) {
    def withValue(newValue: VertexValue): VertexMetadata =
      VertexMetadata(newValue, hasOutgoingEdges, oldValue)

    def swapValue(newValue: VertexValue): VertexMetadata =
      VertexMetadata(newValue, hasOutgoingEdges, value)

    def resetValue(newValue: VertexValue): VertexMetadata =
      VertexMetadata(newValue, hasOutgoingEdges)
  }
  type WorkingPageRankGraph = Graph[VertexMetadata, EdgeWeight]

  val DefaultTeleportProb: Double = 0.15
  val DefaultMaxIterations: Int = 100
  val DefaultConvergenceThreshold: Option[Double] = None
  val EPS: Double = 1.0E-15 // machine epsilon: http://en.wikipedia.org/wiki/Machine_epsilon

  /**
   * Runs PageRank using the GraphX Pregel API.
   *
   * Requirements of the input graph (enforced):
   *  - Has no self-referencing nodes (i.e. edges where in and out nodes are the
   *    same)
   *  - Vertex values are normalized (i.e. prior vector is normalized)
   *  - Edge weights are already normalized (i.e. all outgoing edges sum to
   *    `1.0`)
   *
   * This is a partially-distributed computation in that only the graph is
   * distributed.
   *
   * @param inputGraph the graph to operate on, with vector metadata as the
   *          starting PageRank score, edge weights (as `Double`)
   * @param teleportProb probability of a random jump in the graph
   * @param maxIterations a threshold on the maximum number of iterations,
   *          irrespective of convergence
   * @param convergenceThreshold a threshold on the change between iterations
   *          which marks convergence
   *
   * @return the PageRank vector
   */
  def run(
    inputGraph: PageRankGraph,
    teleportProb: Double = DefaultTeleportProb,
    maxIterations: Int = DefaultMaxIterations,
    convergenceThreshold: Option[Double] = DefaultConvergenceThreshold): VectorRDD = {

    // TODO: convert these to counts (where possible) so error debugging is easier
    require(hasNoSelfReferences(inputGraph.triplets))
    require(isVectorNormalized(inputGraph.vertices))
    require(areEdgeWeightsNormalized(inputGraph))

    require(teleportProb >= 0.0, "Teleport probability must be greater than or equal to 0.0")
    require(teleportProb < 1.0, "Teleport probability must be less than 1.0")
    require(maxIterations > 0, "Max iterations must be greater than 0")

    convergenceThreshold.map { t =>
      require(t > 0.0, "Convergence threshold must be greater than 0.0")
      require(t < 1.0, "Convergence threshold must less than 1.0")
    }

    var graph = buildWorkingPageRankGraph(inputGraph)

    val sc = graph.vertices.context
    val n = sc.broadcast(inputGraph.numVertices)

    // iterate until convergence
    var hasConverged = false
    var numIterations = 0
    while (!hasConverged && numIterations < maxIterations) {
      /**
       * Closure that updates the PageRank value of a node based on the incoming
       * sum. If the vertex does not have outgoing links (is a "dangling" node),
       * it does not distribute all its PageRank mass (only through the
       * teleport). Hence, update the dangling mass counter. Finally, since we
       * do not want self-references, subtract the old PageRank mass from the
       * new one because we will distribute the missing mass equally among all
       * nodes.
       */
      def updateVertex(vId: VertexId, vMeta: VertexMetadata, incomingSum: Option[VertexValue]): VertexMetadata = {
        var newPageRank = (1 - teleportProb) * incomingSum.getOrElse(0.0) + teleportProb / n.value

        if (!vMeta.hasOutgoingEdges) {
          newPageRank -= (1 - teleportProb) * (1.0 / (n.value - 1.0)) * vMeta.value
          vMeta.swapValue(newPageRank)
        } else {
          vMeta.withValue(newPageRank)
        }
      }

      // save previous graph before computing next iteration
      val previousGraph = graph

      // compute vertex update messages over all edges
      val messages = graph.aggregateMessages[VertexValue](
        ctx => ctx.sendToDst(ctx.srcAttr.value * ctx.attr),
        _ + _
      )

      // update vertices with message sums
      val intermediateGraph = graph.outerJoinVertices(messages)(updateVertex)
      val danglingMass = intermediateGraph.vertices.map(_._2.oldValue).sum()

      // distribute missing mass from dangling nodes equally
      // (not having self-references is accounted for in `updateVertex`)
      val missingMassOnEachVertex = (1 - teleportProb) * (1.0 / (n.value - 1.0)) * danglingMass
      graph = intermediateGraph.mapVertices { case (_, vMeta) =>
        vMeta.resetValue(vMeta.value + missingMassOnEachVertex)
      }
      //graph = graph.persist(StorageLevel.MEMORY_ONLY)

      // check for convergence (if threshold was provided)
      convergenceThreshold.map { t =>
        hasConverged = delta(previousGraph.vertices, graph.vertices) < t
      }
      numIterations += 1
    }

    // return the PageRank vector
    graph.
      vertices.
      mapValues(_.value)
  }

  /**
   * Calculates the per-component change/delta and sums over all components
   * (norm) to determine the total change/delta between the two vectors.
   */
  private[spark] def delta(left: VertexRDD[VertexMetadata], right: VertexRDD[VertexMetadata]): VertexValue = {
    left.
      join(right).
      map { case (_, (l, r)) =>
        math.abs(l.value - r.value)
      }.
      sum
  }

  private[spark] def hasNoSelfReferences(edges: RDD[EdgeTriplet[EdgeWeight, EdgeWeight]]): Boolean =
    edges.filter(e => e.srcId == e.dstId).isEmpty

  private[spark] def isVectorNormalized(vector: VectorRDD): Boolean =
    math.abs(1.0 - vector.map(_._2).sum) <= EPS

  private[spark] def areEdgeWeightsNormalized(graph: PageRankGraph): Boolean = {
    graph.
      collectEdges(EdgeDirection.Out).      // results in a `VertexRDD[Array[Edge[ED]]]`
      map(_._2.map(_.attr).sum).            // count the out edge weights
      filter(x => math.abs(1.0 - x) > EPS). // filter and keep those that are not normalized across out edges
      isEmpty
  }

  /**
   * Attach flag for "has outgoing edges" to produce initial graph.
   */
  private[spark] def buildWorkingPageRankGraph(graph: PageRankGraph): WorkingPageRankGraph = {
    graph.outerJoinVertices(graph.outDegrees) { (_, value, outDegrees) =>
      val hasOutgoingEdges = outDegrees match {
        case None => false
        case Some(x) => x != 0
      }
      VertexMetadata(value, hasOutgoingEdges)
    }
  }
}
