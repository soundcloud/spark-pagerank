package com.soundcloud.spark

import org.apache.spark.graphx.{ Edge, Graph, VertexId }
import org.scalatest.{ FunSuite, Matchers }

import com.soundcloud.spark.test.SparkTesting

class PageRankTest
  extends FunSuite
  with Matchers
  with GraphUtils
  with GraphTesting
  with SparkTesting {

  test("PageRank: simple graph (1)") {
    // https://github.com/purzelrakete/Pagerank.jl/blob/070a193826f6ff7c2fe11fb5556f21d039b82e26/test/data/simple.dot
    // https://github.com/purzelrakete/Pagerank.jl/blob/070a193826f6ff7c2fe11fb5556f21d039b82e26/test/rank/simple.jl
    val teleportProb = 0.5
    val maxIterations = 100
    val convergenceThreshold = Some(math.pow(10, -3))
    val input = Seq(
      (1, 2),
      (3, 2),
      (2, 1),
      (2, 3)
    )
    val expected = Seq((1, 5.0/18), (2, 4.0/9), (3, 5.0/18))
    execTest(input, expected, teleportProb, maxIterations, convergenceThreshold)
  }

  test("PageRank: simple graph (2)") {
    // https://github.com/joshdevins/pagerank/blob/0eede643cefd2869be7be78b57afc372e67749fe/octave/pagerank.m#L204
    val teleportProb = 0.15
    val maxIterations = 100
    val convergenceThreshold = Some(math.pow(10, -3))
    val input = Seq(
      (1, 5),
      (2, 1),
      (3, 1),
      (4, 2),
      (4, 3),
      (5, 3),
      (5, 4)
    )
    val expected = Seq((1, 0.284047), (2, 0.091672), (3, 0.207025), (4, 0.145353), (5, 0.271903))
    execTest(input, expected, teleportProb, maxIterations, convergenceThreshold)
  }

  test("PageRank: graph with dangling node (1)") {
    // https://github.com/joshdevins/pagerank/blob/0eede643cefd2869be7be78b57afc372e67749fe/octave/pagerank.m#L211
    val teleportProb = 0.15
    val maxIterations = 100
    val convergenceThreshold = Some(math.pow(10, -5))
    val input = Seq(
      // 1 has no outgoing edges
      (2, 1),
      (3, 1),
      (4, 2),
      (4, 3),
      (5, 3),
      (5, 4)
    )
    val expected = Seq((1, 0.35766), (2, 0.17021), (3, 0.21517), (4, 0.15096), (5, 0.10600))
    execTest(input, expected, teleportProb, maxIterations, convergenceThreshold)
  }

  test("PageRank: graph with dangling node (2)") {
    val teleportProb = 0.15
    val maxIterations = 100
    val convergenceThreshold = Some(math.pow(10, -5))
    val input = Seq(
      (1, 2),
      (1, 3),
      (1, 5),
      (2, 1),
      (2, 5),
      (3, 1),
      (3, 4),
      (3, 5),
      (5, 1),
      (5, 2),
      (5, 3),
      (6, 2)
      // 4 has no outgoing edges
      // 6 has no incoming edges
    )
    val expected = Seq((1,0.248500), (2,0.210849), (3,0.178671), (4,0.075625), (5,0.248500), (6,0.037856))
    execTest(input, expected, teleportProb, maxIterations, convergenceThreshold)
  }

  test("PageRank: graph with two dangling nodes") {
    val teleportProb = 0.15
    val maxIterations = 100
    val convergenceThreshold = Some(math.pow(10, -5))
    val input = Seq(
      (2, 1),
      (2, 5),
      (3, 1),
      (3, 4),
      (3, 5),
      (5, 1),
      (5, 2),
      (5, 3),
      (6, 2)
      // 1 has no outgoing edges
      // 4 has no outgoing edges
      // 6 has no incoming edges
    )
    val expected = Seq((1, 0.2363), (2, 0.2149), (3, 0.1442), (4, 0.1060), (5, 0.2154), (6, 0.0832))
    execTest(input, expected, teleportProb, maxIterations, convergenceThreshold)
  }

  private def execTest(
    input: Seq[(Int, Int)],
    expected: Seq[(Int, Double)],
    teleportProb: Double,
    maxIterations: Int,
    convergenceThreshold: Option[Double]) = {

    val nodes = input.unzip
    val n = (nodes._1 ++ nodes._2).distinct.size
    val prior = 1.0 / n

    val edges = normalizeOutEdgeWeightsRDD(sc.parallelize(expandEdgeTuples(input)))
    val graph = Graph.fromEdges(edges, prior)
    val actual = PageRank.run(graph, teleportProb, maxIterations, convergenceThreshold).collect()

    // error between each component, expected vs actual
    def squaredError(actual: Seq[(VertexId, Double)], expected: Seq[(Int, Double)]): Double = {
      val expectedMap = expected.toMap
      actual.map { case (id, value) =>
        math.pow(value - expectedMap.getOrElse(id.toInt, 0.0), 2)
      }.sum
    }

    actual.map(_._2).sum shouldBe 1.0 +- EPS
    squaredError(actual, expected) shouldBe 0.0 +- math.pow(10, -5)
  }
}
