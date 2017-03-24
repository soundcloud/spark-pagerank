package com.soundcloud.spark

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{ Edge => XEdge, VertexId => XVertexId }
import org.apache.spark.storage.StorageLevel

/**
 * App to run PageRank on our current graph. This is here temporarily as we
 * work on the implementation.
 */
object PlainPageRankApp extends App {
  val sc = new SparkContext()

  val statsPath = "/user/josh/discorank/stats"
  val edgesPath = "/user/josh/discorank/edges"
  val vertexPath = "/user/josh/discorank/vertices"
  val outputPath = "/user/josh/discorank/pagerank"

  val numPartitions = 4096
  val storageLevel = StorageLevel.MEMORY_AND_DISK

  // TODO(jd): all of the prep vertices should be done once in the "build graph" script

  val edges = sc
    .objectFile[XEdge[Double]](edgesPath)
    .coalesce(numPartitions)
    .map { edge =>
      (edge.srcId, PlainPageRank.Edge(edge.dstId, edge.attr))
    }
    .persist(storageLevel)
  val vertexValues = sc
    .objectFile[(XVertexId, Double)](vertexPath)
    .coalesce(numPartitions)
    .map { case (id, value) =>
      (id, value)
    }
  val vertices = PlainPageRank
    .buildVerticesFromValues(edges, vertexValues)
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

  val pr = PlainPageRank.run(
    edges,
    vertices,
    teleportProb = 0.15,
    maxIterations = 5,
    convergenceThresholdOpt = None,
    numVerticesOpt = Some(numVertices)
  )

  pr.saveAsObjectFile(outputPath)
}
