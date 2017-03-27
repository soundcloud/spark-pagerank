package com.soundcloud.spark.pagerank

import org.apache.spark.storage.StorageLevel
import org.scalatest.{ FunSuite, Matchers }

class PageRankGraphTest
  extends FunSuite
  with Matchers
  with GraphTesting
  with SparkTesting {

  test("uniform priors from edges, without dangle") {
    val input = Seq(
      (1, 5, 1.0),
      (2, 1, 1.0),
      (3, 1, 1.0),
      (4, 2, 1.0),
      (4, 3, 1.0),
      (5, 3, 1.0),
      (5, 4, 1.0)
    )
    val expectedVertices = Seq(
      (1, false),
      (2, false),
      (3, false),
      (4, false),
      (5, false)
    )

    execTestUniformPriorsFromEdges(input, expectedVertices)
  }

  test("uniform priors from edges, with dangle") {
    val input = Seq(
      // node 1 is dangling
      (2, 1, 1.0),
      (3, 1, 1.0),
      (4, 2, 1.0),
      (4, 3, 1.0),
      (5, 3, 1.0),
      (5, 4, 1.0)
    )
    val expectedVertices = Seq(
      (1, true),
      (2, false),
      (3, false),
      (4, false),
      (5, false)
    )

    execTestUniformPriorsFromEdges(input, expectedVertices)
  }

  private def execTestUniformPriorsFromEdges(
      input: Seq[EdgeTuple],
      expectedVertices: Seq[(Int, Boolean)]): Unit = {

    val graph = PageRankGraph.uniformPriorsFromEdges(
      input,
      tmpStorageLevel = StorageLevel.MEMORY_ONLY,
      edgesStorageLevel = StorageLevel.MEMORY_ONLY,
      verticesStorageLevel = StorageLevel.MEMORY_ONLY
    )

    val expectedNumVertices = (input.map(_._1) ++ input.map(_._2)).distinct.size

    graph.numVertices shouldBe expectedNumVertices

    graph.edges.getStorageLevel shouldBe StorageLevel.MEMORY_ONLY
    graph.vertices.getStorageLevel shouldBe StorageLevel.MEMORY_ONLY

    val expectedEdges = input.map { case (srcId, dstId, weight) =>
      (srcId, OutEdge(dstId, weight))
    }
    graph.edges.collect().sortBy(_._1) shouldBe expectedEdges

    val expectedPrior = 1.0 / expectedNumVertices
    val expectedVerticesMapped = expectedVertices.map { case (id, dangle) =>
      (id.toLong, VertexMetadata(expectedPrior, dangle))
    }
    graph.vertices.collect().sortBy(_._1) shouldBe expectedVerticesMapped
  }

}
