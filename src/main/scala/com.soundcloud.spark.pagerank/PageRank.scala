package com.soundcloud.spark.pagerank

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import com.soundcloud.spark.pagerank.GraphUtils._

object PageRank {

  val DefaultTeleportProb: Value = 0.15
  val DefaultMaxIterations: Int = 100

  /**
   * Runs PageRank using Spark's RDD API.
   *
   * This implementation supports weighted edges and "dangling" vertices
   * (no out edges). For performance considerations, the {{edges}} and
   * {{vertices}} must already be cached (ideally in-memory) and this will be
   * verified at runtime. The vertices will be unpersisted and persisted again
   * at the same {{StorageLevel}} as they are mutated after each iteration.
   * Since PageRank is iterative, but builds upon mutating these vertices, we
   * also make use of RDD local checkpointing. This requires that Spark's
   * dynamic allocation feature not be used, and this will be checked at
   * runtime.
   *
   * The structural requirements of the input graph are not enforced at runtime
   * but can be checking using supporting methods. To ensure a proper graph,
   * it is also suggested to use the supporting graph building methods. In
   * general, the requirements for the graph that we expect are:
   *  - All vertex IDs referenced in `edges` exist in `vertices`
   *  - Graph has no self-referencing nodes (i.e. edges where in and out nodes
   *    are the same)
   *  - Vertex values are normalized (i.e. sum to `1.0`)
   *  - Edge weights are normalized (i.e. outgoing edge values sum to `1.0`)
   *
   * @param graph the prepared {{PageRankGraph}} to operate on
   * @param teleportProb probability of a random jump in the graph
   * @param maxIterations a threshold on the maximum number of iterations,
   *          irrespective of convergence
   * @param convergenceThresholdOpt an optional threshold on the change between
   *          iterations which marks convergence (NOTE: providing this will
   *          cause an extra computation after each iteration, so if performance
   *          is of concern, do not provide a value here)
   *
   * @return the PageRank vector
   */
  def run(
    graph: PageRankGraph,
    teleportProb: Value = DefaultTeleportProb,
    maxIterations: Int = DefaultMaxIterations,
    convergenceThresholdOpt: Option[Value] = None): VertexRDD = {

    require(graph.edges.getStorageLevel != StorageLevel.NONE, "Storage level of edges cannot be `NONE`")
    require(graph.vertices.getStorageLevel != StorageLevel.NONE, "Storage level of vertices cannot be `NONE`")

    require(teleportProb >= 0.0, "Teleport probability must be greater than or equal to 0.0")
    require(teleportProb < 1.0, "Teleport probability must be less than 1.0")
    require(maxIterations >= 1, "Max iterations must be greater than or equal to 1")
    convergenceThresholdOpt.map { t =>
      require(t > 0.0, "Convergence threshold must be greater than 0.0")
      require(t < 1.0, "Convergence threshold must less than 1.0")
    }

    // local RDD checkpointing cannot be used with dynamic allocation
    // see below for when this is used
    require(
      dynamicAllocationDisabled(graph.vertices.context.getConf),
      "Executor dynamic allocation must be off since this uses RDD local checkpointing"
    )

    // iterate until the maximum number of iterations or until convergence (optional)
    var newVertices = graph.vertices
    var hasConverged = false
    var numIterations = 0
    while (!hasConverged && numIterations < maxIterations) {

      // save the vertices before the iteration starts in order to check for
      // convergence after the iteration
      val previousVertices = newVertices

      // perform a single PageRank iteration
      // this will create new vertices
      // persist the new vertices at the same level as the previous vertices
      newVertices =
        iterate(graph.edges, previousVertices, teleportProb, graph.numVertices)
          .persist(previousVertices.getStorageLevel)

      // check for convergence (if threshold was provided)
      convergenceThresholdOpt.map { t =>
        hasConverged = delta(previousVertices, newVertices) < t
      }

      // finished with the previousVertices, so unpersist them
      previousVertices.unpersist()

      // now that we have one iteration done, also checkpoint the current
      // vertices to remove the RDD parent lineage
      newVertices.localCheckpoint()

      numIterations += 1
    }

    // vertices from the last iteration performed, but simplified back to VertexRDD
    newVertices.map { case (id, meta) =>
      Vertex(id, meta.value)
    }
  }

  /**
   * Checks to see if executor dynamic allocation is off by introspecting the
   * Spark configuration. If the configuration value is not explicitly set, we
   * assume that dynamic allocation is disabled.
   */
  private[pagerank] def dynamicAllocationDisabled(conf: SparkConf): Boolean =
    !conf.getBoolean("spark.dynamicAllocation.enabled", false)

  /**
   * A single PageRank iteration.
   */
  private[pagerank] def iterate(
      edges: OutEdgePairRDD,
      vertices: RichVertexPairRDD,
      teleportProb: Value,
      numVertices: Id): RichVertexPairRDD = {

    /**
     * Calculates the new PageRank value of a vertex given the incoming
     * probability mass plus the teleport probability.
     */
    def calculateVertexUpdate(incomingSum: Value): Value =
      ((1 - teleportProb) * incomingSum) + (teleportProb / numVertices)

    /**
     * Calculates the dangling node delta update, after the normal vertex
     * update. Note the `n - 1` is to account for no self-references in this
     * node we are updating.
     */
    def calculateDanglingVertexUpdate(startingValue: Value): Value =
      (1 - teleportProb) * (1.0 / (numVertices - 1)) * startingValue

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
    def updateVertex(
        perVertexMissingMass: Value,
        vMeta: VertexMetadata,
        incomingSumOpt: Option[Value]): VertexMetadata = {

      val incomingSum = incomingSumOpt.getOrElse(0.0)
      val newValue = calculateVertexUpdate(incomingSum) + perVertexMissingMass

      if (vMeta.isDangling)
        vMeta.withNewValue(newValue - calculateDanglingVertexUpdate(vMeta.value))
      else
        vMeta.withNewValue(newValue)
    }

    // collect what *will be* the dangling mass after the update that follows
    val danglingMass =
      vertices
        .filter(_._2.isDangling)
        .map(_._2.value)
        .sum()

    // send weights along edges to destinations
    val incomingSumPerVertex = edges
      .join(vertices)
      .map { case(_, (edge, vMeta)) =>
        // sends a proportional amount of mass to the destination
        (edge.dstId, vMeta.value * edge.weight)
      }
      .reduceByKey(_ + _)

    // distribute missing mass from dangling nodes equally to all other nodes
    val perVertexMissingMass = calculateDanglingVertexUpdate(danglingMass)

    // update the vertices with the transferred mass, plus teleport, plus dangling mass (where applicable)
    vertices
      .leftOuterJoin(incomingSumPerVertex)
      .map { case(vId, (vMeta, incomingSumOpt)) =>
        (vId, updateVertex(perVertexMissingMass, vMeta, incomingSumOpt))
      }
  }

  /**
   * Calculates the per-component change/delta and sums over all components
   * (norm) to determine the total change/delta between the two vectors.
   */
  private[pagerank] def delta(left: RichVertexPairRDD, right: RichVertexPairRDD): Value = {
    left
      .join(right)
      .map { case (_, (l, r)) =>
        math.abs(l.value - r.value)
      }
      .sum()
  }
}
