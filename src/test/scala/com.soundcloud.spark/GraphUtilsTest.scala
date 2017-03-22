package com.soundcloud.spark

import org.apache.spark.graphx.Graph
import org.scalatest.{ FunSuite, Matchers }

import com.soundcloud.spark.GraphUtils._
import com.soundcloud.spark.test.SparkTesting

class GraphUtilsTest
  extends FunSuite
  with Matchers
  with GraphTesting
  with SparkTesting {

  test("count dangling vertices") {
    val fixtures = Seq(
      (
        Seq(
          (1, 2),
          (3, 2),
          (2, 1),
          (2, 3)
        ),
        0
      ),
      (
        Seq(
          (2, 1),
          (3, 1),
          (4, 2),
          (4, 3),
          (5, 3),
          (5, 4)
          // 1 has no outgoing edges
          // 5 has no incoming edges
        ),
        1
      ),
      (
        Seq(
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
        ),
        2
      )
    )

    fixtures.foreach { case(input, expected) =>
      val rdd = sc.parallelize(expandEdgeTuples(input))
      countDanglingVertices(rdd) shouldBe expected
    }
  }

  test("count self-references") {
    val fixtures = Seq(
      (
        Seq(
          (1, 3),
          (3, 1),
          (4, 2),
          (4, 3),
          (5, 3),
          (5, 4)
        ),
        0
      ),
      (
        Seq(
          (1, 1),
          (3, 1),
          (4, 2),
          (4, 3),
          (5, 3),
          (5, 4)
        ),
        1
      ),
      (
        Seq(
          (1, 1),
          (3, 1),
          (4, 2),
          (4, 4),
          (5, 3),
          (5, 4)
        ),
        2
      )
    )

    fixtures.foreach { case(input, expected) =>
      val rdd = sc.parallelize(expandEdgeTuples(input))
      countSelfReferences(rdd) shouldBe expected
    }
  }

  test("count vertices without normalized out edges") {
    val fixtures = Seq(
      (
        Seq(
          (1, 4, 1.0),
          (3, 1, 1.0),
          (4, 2, 7.0/10.0),
          (4, 3, 3.0/10.0),
          (5, 3, 2.0/5.0),
          (5, 4, 3.0/5.0)
        ),
        0
      ),
      (
        Seq(
          (1, 4, 1.0),
          (3, 1, 1.0),
          (4, 2, 7.0),
          (4, 3, 3.0),
          (5, 3, 2.0/5.0),
          (5, 4, 3.0/5.0)
        ),
        1
      )
    )

    fixtures.foreach { case(input, expected) =>
      val rdd = sc.parallelize(expandEdgeTuples(input))
      countVerticesWithoutNormalizedOutEdges(rdd) shouldBe expected
    }
  }

  test("are vertices normalized") {
    val fixtures = Seq(
      (Seq(0.1, 0.3, 0.3, 0.1, 0.2), true),
      (Seq(1.0), true),
      (Seq(0.0), false),
      (Seq(1.0, 1.0), false),
      (Seq(0.1, 0.1, 0.1), false)
    )

    fixtures.foreach { case(input, expected) =>
      val rdd = sc.parallelize(input.zipWithIndex.map(x => (x._2, x._1)))
      areVerticesNormalized(rdd) shouldBe expected
    }
  }

  test("normalize out edge weights") {
    val fixtures = Seq(
      (
        Seq(
          (1, 4, 0.2),
          (3, 1, 0.5),
          (4, 2, 7.0),
          (4, 3, 3.0),
          (5, 3, 2.0),
          (5, 4, 3.0)
        ),
        Seq(
          (1, 4, 1.0),
          (3, 1, 1.0),
          (4, 2, 7.0/10.0),
          (4, 3, 3.0/10.0),
          (5, 3, 2.0/5.0),
          (5, 4, 3.0/5.0)
        )
      ),
      (
        Seq(
          (1, 11, 3.0),
          (1, 12, 5.0),
          (1, 13, 2.0),
          (2, 21, 10.0),
          (2, 22, 30.0)
        ),
        Seq(
          (1, 11, 3.0/10.0),
          (1, 12, 5.0/10.0),
          (1, 13, 2.0/10.0),
          (2, 21, 10.0/40.0),
          (2, 22, 30.0/40.0)
        )
      )
    )

    fixtures.foreach { case(input, expected) =>
      val rdd = sc.parallelize(expandEdgeTuples(input))
      val normEdges = normalizeOutEdgeWeightsRDD(rdd).collect()
      val actual = collapseEdges(normEdges)

      actual.sorted shouldBe expected.sorted
    }
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
