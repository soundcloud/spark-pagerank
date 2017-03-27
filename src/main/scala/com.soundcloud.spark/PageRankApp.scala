package com.soundcloud.spark

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{ Edge => XEdge, VertexId => XVertexId }
import org.apache.spark.storage.StorageLevel

/**
 * App to run PageRank on our current graph. This is here temporarily as we
 * work on the implementation.
 */
object PageRankApp extends App {
  val sc = new SparkContext()

  val statsPath = "/user/josh/discorank/stats"
  val edgesPath = "/user/josh/discorank/edges"
  val vertexPath = "/user/josh/discorank/vertices"
  val outputPath = "/user/josh/discorank/pagerank"

  val numPartitions = 4096
  val storageLevel = StorageLevel.MEMORY_AND_DISK

  val edges = sc
    .objectFile[XEdge[Double]](edgesPath)
    .coalesce(numPartitions)
    .map { edge =>
      (edge.srcId, PageRank.Edge(edge.dstId, edge.attr))
    }
    .persist(storageLevel)
  val vertexValues = sc
    .objectFile[(XVertexId, Double)](vertexPath)
    .coalesce(numPartitions)
  val vertices = PageRank
    .buildVerticesFromVertexValues(edges, vertexValues)
    .persist(storageLevel)

  val numVertices = sc
    .textFile(statsPath)
    .collect()
    .map { x =>
      val Array(k, v) = x.split("\t")
      (k, v.toLong)
    }
    .filter(_._1 == "numVertices")
    .head
    ._2
  val prior = 1.0 / numVertices

  println(s"numVertices: $numVertices")
  println(s"prior: $prior")

  val pr = PageRank.run(
    edges,
    vertices,
    teleportProb = 0.15,
    maxIterations = 5,
    convergenceThresholdOpt = None,
    numVerticesOpt = Some(numVertices)
  )

  pr.saveAsObjectFile(outputPath)
}
