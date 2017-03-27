package com.soundcloud.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import com.soundcloud.spark.GraphUtils._

object PageRank {

  type VertexId = Long
  type VertexValue = Double
  type EdgeValue = Double

  type Edges = RDD[(VertexId, Edge)] // (srcId, Edge) -- optimal for joins
  type Vertices = RDD[(VertexId, VertexMetadata)]

  case class Edge(dstId: VertexId, weight: EdgeValue)
  case class VertexMetadata(value: VertexValue, isDangling: Boolean) {
    /**
     * Copies the vertex properties but provides a new value. This creates a new
     * immutable vertex instance.
     */
    def withNewValue(newValue: VertexValue): VertexMetadata =
      VertexMetadata(newValue, isDangling)
  }

  val DefaultTeleportProb: VertexValue = 0.15
  val DefaultMaxIterations: Int = 100

  /**
   * Builds vertices with uniform values/priors given edges in the graph. This
   * can be used as a starting point for the first iteration of PageRank. This
   * does not add any flag for dangling nodes, which needs to be done after
   * this.
   */
  def buildUniformVertexValues(edges: Edges): (VertexId, RDD[(VertexId, VertexValue)]) = {
    // build vertices and get vertex stats
    val vertexIds = edges
      .flatMap(e => Seq(e._1, e._2.dstId))
      .distinct()

    val numVertices = vertexIds.count()
    val prior = 1.0 / numVertices

    val rdd = vertexIds.map(id => (id, prior))
    (numVertices, rdd)
  }

  /**
   * Builds vertices RDD from just ID and value pairs, by attaching the dangling
   * flag for vertices with no out-edges.
   */
  def buildVerticesFromVertexValues(
      edges: Edges,
      vertices: RDD[(VertexId, VertexValue)]): Vertices = {

    val a = edges.map(_._1).distinct.map(x => (x, 1)) // srcId
    val b = edges.map(_._2.dstId).distinct.map(x => (x, 1)) // dstId

    val dangles = a
      .cogroup(b)
      .map { case(id, vals) =>
        val isDangling = vals._1.size == 0 && vals._2.size == 1
        (id, isDangling)
      }

    vertices
      .join(dangles)
      .map { case(id, (value, isDangling)) =>
        (id, VertexMetadata(value, isDangling))
      }
  }

  /**
   * Runs PageRank using the RDD APIs of Spark. This supports weighted edges and
   * "dangling" vertices (no out edges). For performance considerations, the
   * {{edges}} and {{vertices}} must already be cached (ideally in-memory) and
   * this will be verified at runtime. The vertices will be unpersisted and
   * persisted again at the same {{StorageLevel}} as they are mutated after each
   * iteration.
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
   * @param edges the edges of the graph
   * @param vertices the vertices of the graph
   * @param teleportProb probability of a random jump in the graph
   * @param maxIterations a threshold on the maximum number of iterations,
   *          irrespective of convergence
   * @param convergenceThresholdOpt an optional threshold on the change between
   *          iterations which marks convergence (NOTE: providing this will
   *          cause an extra computation after each iteration, so if performance
   *          is of concern, do not provide a value here)
   * @param numVerticesOpt the number of vertices in the input graph calculated
   *          lazily from {{vertices}} if none is provided here (requires one
   *          extra job so this is an optimization if you already have this
   *          value, for example from building vertices with a uniform value
   *          distribution for a prior)
   *
   * @return the PageRank vector
   */
  def run(
    edges: Edges,
    vertices: Vertices,
    teleportProb: VertexValue = DefaultTeleportProb,
    maxIterations: Int = DefaultMaxIterations,
    convergenceThresholdOpt: Option[VertexValue] = None,
    numVerticesOpt: Option[VertexId] = None): Vertices = {

    require(edges.getStorageLevel != StorageLevel.NONE, "Storage level of edges cannot be `NONE`")
    require(vertices.getStorageLevel != StorageLevel.NONE, "Storage level of vertices cannot be `NONE`")

    require(teleportProb >= 0.0, "Teleport probability must be greater than or equal to 0.0")
    require(teleportProb < 1.0, "Teleport probability must be less than 1.0")
    require(maxIterations >= 1, "Max iterations must be greater than or equal to 1")

    convergenceThresholdOpt.map { t =>
      require(t > 0.0, "Convergence threshold must be greater than 0.0")
      require(t < 1.0, "Convergence threshold must less than 1.0")
    }

    numVerticesOpt.map { n =>
      require(n >= 2, "Number of vertices must be greater than or equal to 2")
    }

    // lazily calculated when needed, constant over all iterations
    val numVertices = numVerticesOpt.getOrElse(vertices.count())

    // iterate until the maximum number of iterations or until convergence (optional)
    var newVertices = vertices
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
        iterate(edges, previousVertices, teleportProb, numVertices)
          .persist(previousVertices.getStorageLevel)

      // check for convergence (if threshold was provided)
      convergenceThresholdOpt.map { t =>
        hasConverged = delta(previousVertices, newVertices) < t
      }

      // finished with the previousVertices, so unpersist them
      previousVertices.unpersist()

      numIterations += 1
    }

    // vertices from the last iteration performed
    newVertices
  }

  /**
   * A single PageRank iteration.
   */
  private[spark] def iterate(
      edges: Edges,
      vertices: Vertices,
      teleportProb: VertexValue,
      numVertices: VertexId): Vertices = {

    /**
     * Calculates the new PageRank value of a vertex given the incoming
     * probability mass plus the teleport probability.
     */
    def calculateVertexUpdate(incomingSum: VertexValue): VertexValue =
      ((1 - teleportProb) * incomingSum) + (teleportProb / numVertices)

    /**
     * Calculates the dangling node delta update, after the normal vertex
     * update. Note the `n - 1` is to account for no self-references in this
     * node we are updating.
     */
    def calculateDanglingVertexUpdate(startingValue: VertexValue): VertexValue =
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
        perVertexMissingMass: VertexValue,
        vMeta: VertexMetadata,
        incomingSumOpt: Option[VertexValue]): VertexMetadata = {

      val incomingSum = incomingSumOpt.getOrElse(0.0)
      val newValue = calculateVertexUpdate(incomingSum) + perVertexMissingMass

      if (vMeta.isDangling)
        vMeta.withNewValue(newValue - calculateDanglingVertexUpdate(vMeta.value))
      else
        vMeta.withNewValue(newValue)
    }

    // collect what *will be* the dangling mass after the update that follows
    val danglingMass =
      vertices.
        filter(_._2.isDangling).
        map(_._2.value).
        sum()

    // send weights along edges to destinations
    val incomingSumPerVertex = edges
      .join(vertices)
      .map { case(_, (edge, vMeta)) =>
        // sends a proportional amount of mass to the destination
        (edge.dstId, vMeta.value * edge.weight)
      }.
      reduceByKey(_ + _)

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
  private[spark] def delta(left: Vertices, right: Vertices): VertexValue = {
    left.
      join(right).
      map { case (_, (l, r)) =>
        math.abs(l.value - r.value)
      }.
      sum()
  }
}
