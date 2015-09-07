package com.soundcloud.spark

import org.apache.spark.graphx.{ Edge, Graph }
import org.scalatest.{ FunSuite, Matchers }

import com.soundcloud.spark.test.SparkTesting

class GraphUtilsTest
  extends FunSuite
  with Matchers
  with GraphUtils
  with GraphTesting
  with SparkTesting {

  test("normalize out edge weights") {
    val input = Seq(
      (1, 4, 0.2),
      (3, 1, 0.5),
      (4, 2, 7.0),
      (4, 3, 3.0),
      (5, 3, 2.0),
      (5, 4, 3.0)
    )
    val expected = Seq(
      (1, 4, 1.0),
      (3, 1, 1.0),
      (4, 2, 0.7),
      (4, 3, 0.3),
      (5, 3, 2.0/5),
      (5, 4, 3.0/5)
    )

    val graph = graphFromEdges(input, 1.0)
    val normGraph = normalizeOutEdgeWeights(graph)
    val actual = collapseEdges(normGraph.edges.collect())

    actual shouldBe expected
  }

  test("remove self-references") {
    val input = Seq(
      (1, 2),
      (2, 2), // has one other reference
      (3, 3)
    )
    val expectedEdges = Seq(
      (1, 2)
    )
    val expectedVertices = Seq(1, 2, 3)

    val graph = removeSelfReferences(graphFromEdges(input))

    collapseEdgesWithoutValue(graph.edges.collect()) shouldBe expectedEdges
    graph.vertices.collect().map(_._1.toInt).sorted shouldBe expectedVertices
  }

  test("remove disconnected vertices") {
    val edges = Seq(
      (1, 2),
      (3, 4),
      (3, 1),
      (4, 2)
    ).sorted
    val vertices =
      Set[Long](1, 2, 3, 4, 100, 101).map((_, 0.0)).toSeq

    val graph = Graph(
      vertices = sc.parallelize(vertices),
      edges = sc.parallelize(expandEdgeTuples(edges))
    )

    val newGraph = removeDisconnectedVertices(graph)

    collapseEdgesWithoutValue(newGraph.edges.collect()).sorted shouldBe edges // unchanged
    newGraph.vertices.collect().map(_._1).toSeq.sorted shouldBe (1 to 4).toSeq
  }
}
