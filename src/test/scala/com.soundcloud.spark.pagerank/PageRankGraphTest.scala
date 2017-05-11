package com.soundcloud.spark.pagerank

import org.apache.spark.storage.StorageLevel
import org.scalatest.{ Matchers, FunSuite }

class PageRankGraphTest
  extends FunSuite
  with Matchers
  with GraphTesting
  with SparkTesting {

  test("update vertex values, exact same number of vertices, does not need normalization") {
    val newGraph = simpleGraph.updateVertexValues(spark.sparkContext.parallelize(Seq(
      Vertex(1, 0.01),
      Vertex(2, 0.02),
      Vertex(3, 0.02),
      Vertex(4, 0.15),
      Vertex(5, 0.80)
    )))

    newGraph.vertices.collect().sortBy(_._1) shouldBe Seq(
      (1, VertexMetadata(0.01, true)),
      (2, VertexMetadata(0.02, false)),
      (3, VertexMetadata(0.02, false)),
      (4, VertexMetadata(0.15, false)),
      (5, VertexMetadata(0.80, false))
    )
  }

  test("update vertex values, fewer vertices in graph, needs normalization") {
    val vertices = Seq(
      Vertex(1, 0.01),
      Vertex(2, 0.02),
      Vertex(3, 0.02),
      Vertex(4, 0.15),
      Vertex(5, 0.70),
      Vertex(6, 0.10) // not in graph
    )
    val newGraph = simpleGraph.updateVertexValues(spark.sparkContext.parallelize(vertices))

    val totalDelta = 0.10
    val perNodeDelta = totalDelta / 5
    val expectedValues = vertices.map(_.value + perNodeDelta)

    val actualValues = newGraph.vertices.collect().sortBy(_._1).map(_._2.value)

    actualValues.zip(expectedValues).foreach { case (actual, expected) =>
      actual shouldBe (expected) +- EPS
    }
  }

  test("update vertex values, more vertices in graph") {
    val vertices = Seq(
      Vertex(1, 0.01),
      Vertex(2, 0.02),
      Vertex(3, 0.02),
      Vertex(4, 0.15)
      // Vertex(5, 0.70) // in the graph, but not updated
    )
    val newGraph = simpleGraph.updateVertexValues(spark.sparkContext.parallelize(vertices))

    // vertex 5 will have value 1.0/5 = 0.20
    // sum of all new values is also 0.20
    val totalDelta = 0.60
    val perNodeDelta = totalDelta / 5
    val expectedValues = vertices.map(_.value + perNodeDelta)

    val actualValues = newGraph.vertices.collect().sortBy(_._1).map(_._2.value)

    actualValues.zip(expectedValues).foreach { case (actual, expected) =>
      actual shouldBe (expected) +- EPS
    }
  }

  test("validate structure") {
    // this just needs to test converstion to plain graph parts,
    // the rest is tested in GraphUtilsTest

    // no problems
    simpleGraph.validateStructure() shouldBe None

    // just one problem, non-normalized edges
    val nonNormalizedGraph = PageRankGraph.fromEdgesWithUniformPriors(
      simpleEdges,
      tmpStorageLevel = StorageLevel.NONE,
      edgesStorageLevel = StorageLevel.NONE,
      verticesStorageLevel = StorageLevel.NONE
    )
    val errorsOpt = nonNormalizedGraph.validateStructure()
    errorsOpt.isEmpty shouldBe false
    val errors = errorsOpt.get
    errors.size shouldBe 1
    errors.head.contains("normalized") shouldBe true
  }

  test("save and load graph") {
    val edges = Seq[OutEdgePair](
      // node 1 is dangling
      (2, OutEdge(1, 1.0)),
      (3, OutEdge(1, 1.0)),
      (4, OutEdge(2, 1.0)),
      (4, OutEdge(3, 1.0)),
      (5, OutEdge(3, 1.0)),
      (5, OutEdge(4, 1.0))
    )
    val vertices = Seq[RichVertexPair](
      (1, VertexMetadata(0.1, true)),
      (2, VertexMetadata(0.2, false)),
      (3, VertexMetadata(0.3, false)),
      (4, VertexMetadata(0.4, false)),
      (5, VertexMetadata(0.5, false))
    )
    val graph = PageRankGraph(
      numVertices = vertices.size,
      edges = spark.sparkContext.parallelize(edges),
      vertices = spark.sparkContext.parallelize(vertices)
    )

    val path = "target/test/PageRankGraphTest"
    PageRankGraph.save(spark, graph, path)

    val loadedGraph = PageRankGraph.load(spark, path, StorageLevel.NONE, StorageLevel.NONE)

    // compare components
    loadedGraph.numVertices shouldBe graph.numVertices
    loadedGraph.edges.collect() shouldBe edges
    loadedGraph.vertices.collect() shouldBe vertices
  }

  test("graph from edges with, uniform priors, without dangle") {
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

    execTestFromEdgesWithUniformPriors(input, expectedVertices)
  }

  test("graph from edges, with uniform priors, with dangle") {
    val expectedVertices = Seq(
      (1, true),
      (2, false),
      (3, false),
      (4, false),
      (5, false)
    )

    execTestFromEdgesWithUniformPriors(simpleEdges, expectedVertices)
  }

  private def simpleEdges: Seq[EdgeTuple] = {
    Seq(
      // node 1 is dangling
      (2, 1, 1.0),
      (3, 1, 1.0),
      (4, 2, 1.0),
      (4, 3, 1.0),
      (5, 3, 1.0),
      (5, 4, 1.0)
    )
  }

  private def simpleGraph: PageRankGraph = {
    PageRankGraph.fromEdgesWithUniformPriors(
      GraphUtils.normalizeOutEdgeWeights(simpleEdges),
      tmpStorageLevel = StorageLevel.MEMORY_ONLY,
      edgesStorageLevel = StorageLevel.MEMORY_ONLY,
      verticesStorageLevel = StorageLevel.MEMORY_ONLY
    )
  }

  private def execTestFromEdgesWithUniformPriors(
      input: Seq[EdgeTuple],
      expectedVertices: Seq[(Int, Boolean)]): Unit = {

    val graph = PageRankGraph.fromEdgesWithUniformPriors(
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
