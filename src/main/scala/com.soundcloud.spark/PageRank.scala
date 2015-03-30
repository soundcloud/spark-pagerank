package com.soundcloud.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx.{ EdgeRDD, Graph, VertexRDD }

object PageRank {
  type PageRankGraph = Graph[Double, Double]
  type VectorRDD = VertexRDD[Double]

  val DefaultTeleportProb: Double = 0.15
  val DefaultMaxIterations: Int = 100
  val DefaultConvergenceThreshold: Option[Double] = None
  val EPS: Double = 1.0E-15 // machine epsilon: http://en.wikipedia.org/wiki/Machine_epsilon

  /**
   * Runs the PageRank algorithm. Edge weights must be normalized (i.e. all
   * outgoing edges sum to `1.0`). This is a partially-distributed computation
   * in that only the graph is distributed.
   *
   * @param inputGraph the graph to operate on, with vector metadata as the starting PageRank score, edge weights (as `Double`)
   * @param teleportProb probability of a random jump in the graph
   * @param maxIterations a threshold on the maximum number of iterations, irrespective of convergence
   * @param convergenceThreshold a threshold on the change between iterations which marks convergence
   *
   * @return the PageRank vector
   */
  def run(
    sc: SparkContext,
    inputGraph: PageRankGraph,
    teleportProb: Double = DefaultTeleportProb,
    maxIterations: Int = DefaultMaxIterations,
    convergenceThreshold: Option[Double] = DefaultConvergenceThreshold): VectorRDD = {

    require(vectorIsNormalized(inputGraph))
    require(edgeWeightsAreNormalized(inputGraph))

    require(teleportProb >= 0.0 "Teleport probability must be greater than or equal to 0.0")
    require(teleportProb < 1.0 "Teleport probability must be less than 1.0")
    require(maxIterations > 0, "Max iterations must be greater than 0")

    convergenceThreshold.foreach {
      require(convergenceThreshold > 0.0, "Convergence threshold must be greater than 0.0")
      require(convergenceThreshold <= 1.0, "Convergence threshold must less than or equal to 1.0")
    }

    val n = sc.broadcast(inputGraph.numVertices)

    // attach flag for has outgoing edges to produce initial graph
    var graph: Graph[(Double, Boolean), Double] = findHasOutgoing(inputGraph)

    // Accumulator for page rank mass on nodes without outgoing links ("dangling" nodes).
    // This mass has to be distributed among all other nodes in an separate step.
    // TODO: it is wrong to use this because Accumulators are currently approximate and might overestimate
    val oldDanglingMassAcc = sc.accumulator(0.0)(SparkContext.DoubleAccumulatorParam)

    // loop
    var (converged, iterations) = (false, 0)
    while (!converged && iterations < maxIterations) {

      // reset accumulator
      oldDanglingMassAcc.setValue(0.0)

      // compute messages on all edges
      val messages = graph.mapReduceTriplets(sendMessage, mergeMessages, Some((graph.vertices, EdgeDirection.Out)))

      /**
       * Closure that updates the page rank of a node based on the incoming sum.
       * If the vertex does not have outgoing links (is a "dangling" node), it
       * does not distribute all its page rank mass (only through the teleport).
       * Hence, update the oldDanglingMassAcc counter. Finally, since we do not
       * want self-references, subtract the old page rank mass from the new one
       * because we will distribute the missing mass equally among all nodes.
       */
      def updateVertex(vId: VertexId, attr: (Double, Boolean), incomingSum: Option[Double]): (Double, Boolean) = {
        var newPageRank = (1 - teleportProb) * incomingSum.getOrElse(0.0) + teleportProb / n.value

        val (oldPageRank, hasOutgoing) = attr
        if (!hasOutgoing) {
          oldDanglingMassAcc.add(oldPageRank)
          newPageRank -= (1 - teleportProb) * (1.0 / (n.value - 1)) * oldPageRank
        }

        (newPageRank, hasOutgoing)
      }

      // update vertices with message sums
      val previousGraph = graph
      val intermediateGraph = graph.outerJoinVertices(messages)(updateVertex)

      // distribute missing mass from dangling nodes equally
      // (not having self-references is accounted for in updateVertex)
      val missingMassOnEachVertex = (1-teleportProb) * (1.0/(n.value-1)) * oldDanglingMassAcc.value
      graph = intermediateGraph.mapVertices{ case (vId, (pr, hasIncoming)) => (pr + missingMassOnEachVertex, hasIncoming) }
      graph = graph.persist(StorageLevel.MEMORY_ONLY)

      // check for convergence, increment iteration counter
      val delta = l1Norm(previousGraph.vertices, graph.vertices)
      converged = delta < convergenceThreshold
      iterations += 1
    }

    // return the PageRank vector
    graph.mapVertices{ case (vId, (pr, _)) => pr }.vertices
  }

  private[spark] def vectorIsNormalized(vector: VectorRDD): Boolean =
    vector.map(_._2).sum < EPS

  private[spark] def countNonNormalizedVertices(graph: PageRankGraph): Int = {
    graph.
      collectEdges(EdgeDirection.OUT). // results in a `VertexRDD[Array[Edge[Double]]]`
      filter { case (_, edges) =>
        edges.map(_.attr).sum > EPS
      }.
      count
  }

  /**
   * Compute transitioning page rank mass of one outgoing edge.
   */
  private[spark] def sendMessage(edge: EdgeTriplet[(Double, Boolean), Double]) = {
    // alternatively check self-references once beforehand
    // assert(g.triplets.filter(e => e.srcId == e.dstId).count() == 0, "self references are not allowed in this PageRank implementation")
    assert(edge.srcId != edge.dstId, "vertex %d has a self-references which is not allowed in this PageRank implementation".format(edge.srcId))

    val (srcPageRank, _) = edge.srcAttr
    Iterator((edge.dstId, srcPageRank * edge.attr))
  }

  /**
   * Called to sum up multiple incoming messages (from sendMessage) on a vertex.
   * Produces incomingSum in updateVertex.
   */
  private[spark] def mergeMessages(a: Double, b: Double) = a + b
}
