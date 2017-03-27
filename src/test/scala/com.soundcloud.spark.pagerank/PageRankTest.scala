package com.soundcloud.spark.pagerank

import org.apache.spark.storage.StorageLevel
import org.scalatest.{ FunSuite, Matchers }

import com.soundcloud.spark.pagerank.GraphUtils._
import com.soundcloud.spark.test.SparkTesting

class PageRankTest
  extends FunSuite
  with Matchers
  with GraphTesting
  with SparkTesting {

  test("builder: build uniform vertex values") {
    val edges = sc.parallelize(Seq[(PageRank.VertexId, PageRank.Edge)](
      // node 1 is dangling
      (2, PageRank.Edge(1, 1.0)),
      (3, PageRank.Edge(1, 1.0)),
      (4, PageRank.Edge(2, 1.0)),
      (4, PageRank.Edge(3, 1.0)),
      (5, PageRank.Edge(3, 1.0)),
      (5, PageRank.Edge(4, 1.0))
    ))
    val (numVertices, rdd) = PageRank.buildUniformVertexValues(edges)

    val expectedNumVertices = 5
    val expectedPrior = 1.0 / expectedNumVertices

    numVertices shouldBe expectedNumVertices
    rdd.collect().sorted shouldBe Seq(
      (1, expectedPrior),
      (2, expectedPrior),
      (3, expectedPrior),
      (4, expectedPrior),
      (5, expectedPrior)
    )
  }

  test("builder: build vertices from just vertex values") {
    val edges = sc.parallelize(Seq[(PageRank.VertexId, PageRank.Edge)](
      // node 1 is dangling
      (2, PageRank.Edge(1, 1.0)),
      (3, PageRank.Edge(1, 1.0)),
      (4, PageRank.Edge(2, 1.0)),
      (4, PageRank.Edge(3, 1.0)),
      (5, PageRank.Edge(3, 1.0)),
      (5, PageRank.Edge(4, 1.0))
    ))

    val (numVertices, vertexValues) = PageRank.buildUniformVertexValues(edges)
    val vertices = PageRank.buildVerticesFromVertexValues(edges, vertexValues).collect()
    val prior = vertexValues.first._2 // just use one, they are all the same

    vertices.sortBy(_._1) shouldBe Seq(
      (1, PageRank.VertexMetadata(prior, true)),
      (2, PageRank.VertexMetadata(prior, false)),
      (3, PageRank.VertexMetadata(prior, false)),
      (4, PageRank.VertexMetadata(prior, false)),
      (5, PageRank.VertexMetadata(prior, false))
    )
  }

  test("PageRank: graph without weights, without dangling (1)") {
    // https://github.com/purzelrakete/Pagerank.jl/blob/070a193826f6ff7c2fe11fb5556f21d039b82e26/test/data/simple.dot
    // https://github.com/purzelrakete/Pagerank.jl/blob/070a193826f6ff7c2fe11fb5556f21d039b82e26/test/rank/simple.jl
    val teleportProb = 0.5
    val maxIterations = 100
    val convergenceThreshold = Some(math.pow(10, -3))
    val input = Seq(
      (1, 2, 1.0),
      (3, 2, 1.0),
      (2, 1, 1.0),
      (2, 3, 1.0)
    )
    val expected = Seq((1, 5.0/18), (2, 4.0/9), (3, 5.0/18))
    execTest(input, expected, teleportProb, maxIterations, convergenceThreshold)
  }

  test("PageRank: graph without weights, without dangling (2)") {
    // https://github.com/joshdevins/pagerank/blob/0eede643cefd2869be7be78b57afc372e67749fe/octave/pagerank.m#L183
    val teleportProb = 0.15
    val maxIterations = 100
    val convergenceThreshold = Some(math.pow(10, -3))
    val input = Seq(
      (1, 5, 1.0),
      (2, 1, 1.0),
      (3, 1, 1.0),
      (4, 2, 1.0),
      (4, 3, 1.0),
      (5, 3, 1.0),
      (5, 4, 1.0)
    )
    val expected = Seq((1, 0.284047), (2, 0.091672), (3, 0.207025), (4, 0.145353), (5, 0.271903))
    execTest(input, expected, teleportProb, maxIterations, convergenceThreshold)
  }

  test("PageRank: graph with weights, without dangling") {
    // https://github.com/joshdevins/pagerank/blob/0eede643cefd2869be7be78b57afc372e67749fe/octave/pagerank.m#L167
    val teleportProb = 0.15
    val maxIterations = 100
    val convergenceThreshold = Some(math.pow(10, -3))
    val input = Seq(
      (1, 5, 1.0),
      (2, 1, 5.0),
      (3, 1, 5.0),
      (4, 2, 1.0),
      (4, 3, 0.5),
      (5, 3, 0.5),
      (5, 4, 1.0)
    )
    val expected = Seq((1, 0.27278), (2, 0.13122), (3, 0.15506), (4, 0.17889), (5, 0.26204))
    execTest(input, expected, teleportProb, maxIterations, convergenceThreshold)
  }

  test("PageRank: graph without weights, with dangling node (1)") {
    // https://github.com/joshdevins/pagerank/blob/0eede643cefd2869be7be78b57afc372e67749fe/octave/pagerank.m#L191
    val teleportProb = 0.15
    val maxIterations = 100
    val convergenceThreshold = Some(math.pow(10, -5))
    val input = Seq(
      // 1 has no outgoing edges
      (2, 1, 1.0),
      (3, 1, 1.0),
      (4, 2, 1.0),
      (4, 3, 1.0),
      (5, 3, 1.0),
      (5, 4, 1.0)
    )
    val expected = Seq((1, 0.35766), (2, 0.17021), (3, 0.21517), (4, 0.15096), (5, 0.10600))
    execTest(input, expected, teleportProb, maxIterations, convergenceThreshold)
  }

  test("PageRank: graph without weights, with dangling node (2)") {
    val teleportProb = 0.15
    val maxIterations = 100
    val convergenceThreshold = Some(math.pow(10, -5))
    val input = Seq(
      (1, 2, 1.0),
      (1, 3, 1.0),
      (1, 5, 1.0),
      (2, 1, 1.0),
      (2, 5, 1.0),
      (3, 1, 1.0),
      (3, 4, 1.0),
      (3, 5, 1.0),
      (5, 1, 1.0),
      (5, 2, 1.0),
      (5, 3, 1.0),
      (6, 2, 1.0)
      // 4 has no outgoing edges
      // 6 has no incoming edges
    )
    val expected = Seq((1,0.248500), (2,0.210849), (3,0.178671), (4,0.075625), (5,0.248500), (6,0.037856))
    execTest(input, expected, teleportProb, maxIterations, convergenceThreshold)
  }

  test("PageRank: graph without weights, with two dangling nodes") {
    val teleportProb = 0.15
    val maxIterations = 100
    val convergenceThreshold = Some(math.pow(10, -5))
    val input = Seq(
      (2, 1, 1.0),
      (2, 5, 1.0),
      (3, 1, 1.0),
      (3, 4, 1.0),
      (3, 5, 1.0),
      (5, 1, 1.0),
      (5, 2, 1.0),
      (5, 3, 1.0),
      (6, 2, 1.0)
      // 1 has no outgoing edges
      // 4 has no outgoing edges
      // 6 has no incoming edges
    )
    val expected = Seq((1, 0.2363), (2, 0.2149), (3, 0.1442), (4, 0.1060), (5, 0.2154), (6, 0.0832))
    execTest(input, expected, teleportProb, maxIterations, convergenceThreshold)
  }

  test("PageRank: graph with weights, with dangling node") {
    // https://github.com/joshdevins/pagerank/blob/0eede643cefd2869be7be78b57afc372e67749fe/octave/pagerank.m#L175
    val teleportProb = 0.15
    val maxIterations = 100
    val convergenceThreshold = Some(math.pow(10, -5))
    val input = Seq(
      // 1 has no outgoing edges
      (2, 1, 0.5),
      (3, 1, 0.5),
      (4, 2, 1.0),
      (4, 3, 0.5),
      (5, 3, 0.5),
      (5, 4, 1.0)
    )
    val expected = Seq((1, 0.35187), (2, 0.19794), (3, 0.18108), (4, 0.16422), (5, 0.10490))
    execTest(input, expected, teleportProb, maxIterations, convergenceThreshold)
  }

  private def execTest(
    input: Seq[(Int, Int, Double)],
    expected: Seq[(Int, Double)],
    teleportProb: Double,
    maxIterations: Int,
    convergenceThreshold: Option[Double]): Unit = {

    val rdd = sc.parallelize(expandEdgeTuples(input))
    val edges = normalizeOutEdgeWeightsRDD(rdd)
      .map { e =>
        (e.srcId, PageRank.Edge(e.dstId, e.attr))
      }
      .persist(StorageLevel.MEMORY_ONLY)

    val (numVertices, vertexValues) = PageRank
      .buildUniformVertexValues(edges)
    val vertices = PageRank
      .buildVerticesFromVertexValues(edges, vertexValues)
      .persist(StorageLevel.MEMORY_ONLY)

    val actual = PageRank
      .run(
        edges,
        vertices,
        teleportProb,
        maxIterations,
        convergenceThreshold,
        Some(numVertices)
      )
      .collect()

    // error between each component, expected vs actual
    def squaredError(actual: Seq[(Long, PageRank.VertexMetadata)], expected: Seq[(Int, Double)]): Double = {
      val expectedMap = expected.toMap
      actual.map { case (id, vMeta) =>
        math.pow(vMeta.value - expectedMap.getOrElse(id.toInt, 0.0), 2)
      }.sum
    }

    actual.map(_._2.value).sum shouldBe 1.0 +- EPS
    squaredError(actual, expected) shouldBe 0.0 +- math.pow(10, -5)
  }
}
