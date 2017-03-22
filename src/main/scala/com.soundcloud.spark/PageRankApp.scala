package com.soundcloud.spark

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{ Edge, Graph }
import org.apache.spark.storage.StorageLevel

/**
 * App to run PageRank on our current graph. This is here temporarily as we
 * work on the implementation.
 */
object PageRankApp extends App {
  val sc = new SparkContext()

  val inputPath = "/user/josh/discorank/normalizedEdges"
  val outputPath = "/user/josh/discorank/prVector"

  val edges = sc.objectFile[Edge[Double]](inputPath).coalesce(10000)
  val numVertices = 650284017
  val prior = 1.0 / numVertices
  val graph = Graph.fromEdges(
    edges,
    defaultValue = prior,
    vertexStorageLevel = StorageLevel.MEMORY_AND_DISK_2,
    edgeStorageLevel = StorageLevel.MEMORY_AND_DISK_2
  )

  val pr = PageRank.run(
    graph,
    teleportProb = 0.15,
    maxIterations = 5,
    convergenceThresholdOpt = None,
    numVerticesOpt = Some(numVertices)
  )

  pr.saveAsObjectFile(outputPath)
}
