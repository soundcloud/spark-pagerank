package com.soundcloud.spark.pagerank

import org.apache.spark.storage.StorageLevel

final case class PageRankGraph(
  numVertices: Id,
  edges: OutEdgePairRDD,
  vertices: RichVertexPairRDD) {

  // basic validation of the parameters
  // anything that requires computation should not be done in this constructor
  //   to allow the caller to control when and even if it happens
  require(numVertices >= 2, "Number of vertices must be greater than or equal to 2")
}

object PageRankGraph {
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
  def uniformPriorsFromEdges(
    edges: EdgeRDD,
    tmpStorageLevel: StorageLevel,
    edgesStorageLevel: StorageLevel,
    verticesStorageLevel: StorageLevel): PageRankGraph = {

    // extract the source and destination ID sets separately
    val srcIds = edges.map(_.srcId).distinct.persist(tmpStorageLevel)
    val dstIds = edges.map(_.dstId).distinct.persist(tmpStorageLevel)

    // determine the union set of all IDs
    val allIds = (srcIds ++ dstIds).distinct().persist(tmpStorageLevel)

    // count vertices, determine the uniform distribution prior, attach to all vertices
    val numVertices = allIds.count()
    val prior = 1.0 / numVertices
    val vertices = allIds.map(id => (id, prior))

    // determine which vertices are dangling
    val srcIdPairs = srcIds.map(x => (x, 1))
    val dstIdPairs = dstIds.map(x => (x, 1))
    val dangles = srcIdPairs
      .cogroup(dstIdPairs)
      .map { case (id, vals) =>
        val isDangling = (vals._1.size == 0 && vals._2.size == 1)
        (id, isDangling)
      }

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
      .map(v => (v._1, v._2))
      .join(dangles)
      .map { case(id, (value, isDangling)) =>
        (id, VertexMetadata(value, isDangling))
      }
      .persist(verticesStorageLevel)

    PageRankGraph(numVertices, prEdges, prVertices)
  }
}
