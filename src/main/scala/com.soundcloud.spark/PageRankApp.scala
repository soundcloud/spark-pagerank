package com.soundcloud.spark

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{ Edge, Graph, VertexId }
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
  val edges = sc.objectFile[Edge[Double]](edgesPath).coalesce(numPartitions)
  val vertices = sc.objectFile[(VertexId, Double)](vertexPath).coalesce(numPartitions)

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

  val graph = Graph.apply(
    vertices,
    edges,
    defaultVertexAttr = prior,
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
