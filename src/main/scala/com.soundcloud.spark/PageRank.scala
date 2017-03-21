package com.soundcloud.spark

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object PageRank extends GraphUtils {

  type VertexValue = Double
  type EdgeWeight = Double
  type PageRankGraph = Graph[VertexValue, EdgeWeight]
  type VectorRDD = VertexRDD[VertexValue]

  case class VertexMetadata(value: VertexValue, isDangling: Boolean) {
    def withValue(newValue: VertexValue): VertexMetadata =
      VertexMetadata(newValue, isDangling)
  }
  type WorkingPageRankGraph = Graph[VertexMetadata, EdgeWeight]

  val DefaultTeleportProb: Double = 0.15
  val DefaultMaxIterations: Int = 100
  val DefaultConvergenceThreshold: Option[Double] = None

  /**
   * Validates the structure of the input PageRank graph. See: {{#run}}.
   */
  def validateGraphStructure(graph: PageRankGraph) {
    val numSelfReferences = countSelfReferences(graph.edges)
    val verticesAreNormalized = areVerticesNormalized(graph.vertices)
    val numVerticesWithoutNormalizedOutEdges = countVerticesWithoutNormalizedOutEdges(graph.edges)

    require(numSelfReferences == 0, "Number of vertices with self-referencing edges must be 0")
    require(verticesAreNormalized, "Input vertices values must be normalized")
    require(numVerticesWithoutNormalizedOutEdges == 0, "Number of vertices without normalized out edges must be 0")
  }

  /**
   * Runs PageRank using the GraphX API.
   *
   * Requirements of the input graph (not-enforced at runtime, see {{#validateGraphStructure}}):
   *  - Has no self-referencing nodes (i.e. edges where in and out nodes are the
   *    same)
   *  - Vertex values are normalized (i.e. prior vector is normalized)
   *  - Edge weights are already normalized (i.e. all outgoing edges sum to
   *    `1.0`)
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

    require(teleportProb >= 0.0, "Teleport probability must be greater than or equal to 0.0")
    require(teleportProb < 1.0, "Teleport probability must be less than 1.0")
    require(maxIterations > 0, "Max iterations must be greater than 0")

    convergenceThreshold.map { t =>
      require(t > 0.0, "Convergence threshold must be greater than 0.0")
      require(t < 1.0, "Convergence threshold must less than 1.0")
    }

    val numVertices = inputGraph.numVertices // lazily calculated, but constant over all iterations
    var graph = buildWorkingPageRankGraph(inputGraph)

    /**
     * A single PageRank iteration.
     */
    def iterate(graph: WorkingPageRankGraph): WorkingPageRankGraph = {
      /**
       * Calculates the new PageRank value of a vertex given the incoming
       * probability mass plus the teleport probability.
       */
      def calculateVertexUpdate(incomingSum: VertexValue, n: Long): VertexValue =
        ((1 - teleportProb) * incomingSum) + (teleportProb / n)

      /**
       * Calculates the dangling node delta update, after the normal vertex
       * update. Note the `n - 1` is to account for no self-references in this
       * node we are updating.
       */
      def calculateDanglingVertexUpdate(startingValue: VertexValue, n: Long): VertexValue =
        (1 - teleportProb) * (1.0 / (n - 1)) * startingValue

      /**
       * Closure that updates the PageRank value of a node based on the incoming
       * sum/probability mass.
       *
       * If the vertex is a "dangling" (no out edges):
       *  - It does not distribute all its PageRank mass on out edges,
       *    only through the teleport
       *  - Since we do not want self-references, subtract the old PageRank mass
       *    from the new one because we will distribute the missing mass equally
       *    among all nodes.
       */
      def updateVertex(numVertices: Long, perVertexMissingMass: Double)(vId: VertexId, vMeta: VertexMetadata, incomingSumOpt: Option[VertexValue]): VertexMetadata = {
        val incomingSum = incomingSumOpt.getOrElse(0.0)
        val newValue = calculateVertexUpdate(incomingSum, numVertices) + perVertexMissingMass

        if (vMeta.isDangling)
          vMeta.withValue(newValue - calculateDanglingVertexUpdate(vMeta.value, numVertices))
        else
          vMeta.withValue(newValue)
      }

      // compute vertex update messages over all edges
      val messages = graph.aggregateMessages[VertexValue](
        ctx => ctx.sendToDst(ctx.srcAttr.value * ctx.attr),
        _ + _
      )

      // collect what *will be* the dangling mass after the update that follows
      val danglingMass =
        graph.
          vertices.
          filter(_._2.isDangling).
          map(_._2.value).
          sum()

      // distribute missing mass from dangling nodes equally to all other nodes
      val perVertexMissingMass = calculateDanglingVertexUpdate(danglingMass, numVertices)

      // to get the new graph, update vertices with message sums and the per vertex missing mass
      graph.outerJoinVertices(messages)(updateVertex(numVertices, perVertexMissingMass))
    }

    // iterate until convergence
    var hasConverged = false
    var numIterations = 0
    while (!hasConverged && numIterations < maxIterations) {

      // save the graph before the iteration starts in order to check for convergence after the iteration
      val previousGraph = graph

      // perform a single PageRank iteration
      graph = iterate(graph)

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
   * Attach flag for "has outgoing edges" to produce initial graph.
   */
  private[spark] def buildWorkingPageRankGraph(graph: PageRankGraph): WorkingPageRankGraph = {
    graph.outerJoinVertices(graph.outDegrees) { (_, value, outDegrees) =>
      val isDangling = outDegrees match {
        case None => true
        case Some(x) => x == 0
      }
      VertexMetadata(value, isDangling)
    }
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
      sum()
  }
}
