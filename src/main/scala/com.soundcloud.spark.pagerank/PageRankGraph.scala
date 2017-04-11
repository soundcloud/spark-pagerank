package com.soundcloud.spark.pagerank

import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

final case class PageRankGraph(
  numVertices: Id,
  edges: OutEdgePairRDD,
  vertices: RichVertexPairRDD) {

  // basic validation of the parameters
  // anything that requires computation should not be done in this constructor
  //   to allow the caller to control when and even if it happens
  require(numVertices >= 2, "Number of vertices must be greater than or equal to 2")

  /**
   * Updates any of the vertex values for those provided. Not all values must be
   * replaced, and any extra vertices from the input RDD will be discarded. This
   * is useful for setting the priors from a previous run or iteration of
   * PageRank. Since this can cause the vector values to no longer be
   * normalized, this will re-normalize the values after updating them.
   */
  def updateVertexValues(newVertices: VertexRDD, eps: Value = EPS): PageRankGraph = {
    // TODO(jd): if performance is a problem, we can cache this temporarily
    val updatedVertices = this.vertices
      .leftOuterJoin(newVertices.map(_.toPair))
      .map { case (id, (meta, newValueOpt)) =>
        (id, meta.withNewValue(newValueOpt.getOrElse(meta.value)))
      }

    def normalizedVertices: RichVertexPairRDD = {
      val totalDelta = 1.0 - updatedVertices.map(_._2.value).sum()

      // nothing to do, no normalization needed
      if (math.abs(totalDelta) <= EPS)
        return updatedVertices

      val perNodeDelta = totalDelta / this.numVertices
      updatedVertices
        .map { case (id, meta) =>
          (id, meta.withNewValue(meta.value + perNodeDelta))
        }
    }

    val newGraph = PageRankGraph(numVertices, edges, normalizedVertices.persist(vertices.getStorageLevel))
    this.vertices.unpersist()

    newGraph
  }
}

object PageRankGraph {
  /**
   * Saves a graph in it's entirety to durable storage.
   *
   * Note that this uses "object files" and is thus loading the graph requires
   * the samve binary version as Spark and this library that was used to save
   * it.
   */
  def save(graph: PageRankGraph, path: String): Unit = {
    // save graph components
    graph.edges.saveAsObjectFile(s"$path/edges")
    graph.vertices.saveAsObjectFile(s"$path/vertices")

    // save the necessary statistics
    Metadata.save(
      graph.edges.context,
      Seq(("numVertices", graph.numVertices)),
      s"$path/stats"
    )
  }

  /**
   * Loads a graph from durable storage.
   *
   * See: #save for more details
   */
  def load(
    sc: SparkContext,
    path: String,
    edgesStorageLevel: StorageLevel,
    verticesStorageLevel: StorageLevel): PageRankGraph = {

    val numVertices = Metadata.loadAndExtract(sc, s"$path/stats", "numVertices")(_.toLong)
    PageRankGraph(
      numVertices,
      edges = sc.objectFile[OutEdgePair](s"$path/edges").persist(edgesStorageLevel),
      vertices = sc.objectFile[RichVertexPair](s"$path/vertices").persist(verticesStorageLevel)
    )
  }

  /**
   * Given the edges of a graph, builds vertices with uniformly distributed
   * values and dangling nodes flags. This can be used as a starting point for
   * the first iteration of PageRank.
   *
   * Performance note: `edges` are iterated over multiple timed, so please
   * consider persisting it first.
   *
   * @param edges the {{EdgeRDD}} to use as the basis for the graph
   * @param tmpStorageLevel the {{StorageLevel}} to use for temporary datasets
   *          that are created during the graph building process (we recommend
   *          using `MEMORY_ONLY` as the datasets are small)
   * @param edgesStorageLevel the {{StorageLevel}} to use for the final edges
   *          produced
   * @param verticesStorageLevel the {{StorageLevel}} to use for the final
   *          vertices produced
   */
  def fromEdgesWithUniformPriors(
    edges: EdgeRDD,
    tmpStorageLevel: StorageLevel,
    edgesStorageLevel: StorageLevel,
    verticesStorageLevel: StorageLevel): PageRankGraph = {

    // extract vertex ID sets from edges
    // extract the source and destination ID sets separately
    var (srcIds, dstIds) = GraphUtils.unzipDistinct(edges)
    srcIds = srcIds.persist(tmpStorageLevel)
    dstIds = dstIds.persist(tmpStorageLevel)

    // determine the union set of all IDs
    val allIds = (srcIds ++ dstIds).distinct().persist(tmpStorageLevel)

    // count vertices, determine the uniform distribution prior, attach to all vertices
    val numVertices = allIds.count()
    val prior = 1.0 / numVertices
    val vertices = allIds.map(id => (id, prior))

    // tag vertices with dangles
    val dangles = GraphUtils.tagDanglingVertices(srcIds, dstIds)

    // unpersist the temporary datasets
    srcIds.unpersist()
    dstIds.unpersist()
    allIds.unpersist()

    // convert edges to working
    val prEdges = edges
      .map(_.toOutEdgePair)
      .persist(edgesStorageLevel)

    // build the final dataset with the uniform prior and the dangle flags
    val prVertices = vertices
      .join(dangles)
      .map { case(id, (value, isDangling)) =>
        (id, VertexMetadata(value, isDangling))
      }
      .persist(verticesStorageLevel)

    PageRankGraph(numVertices, prEdges, prVertices)
  }
}
