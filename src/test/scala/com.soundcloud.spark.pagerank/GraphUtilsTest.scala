package com.soundcloud.spark.pagerank

import org.scalatest.{ FunSuite, Matchers }

class GraphUtilsTest
  extends FunSuite
  with Matchers
  with GraphTesting
  with SparkTesting {

  test("unzip distinct") {
    val edges = Seq(
      (1, 2, 1.0),
      (3, 2, 1.0),
      (2, 1, 1.0),
      (2, 3, 1.0)
    )

    val (srcIds, dstIds) = GraphUtils.unzipDistinct(edges)
    srcIds.collect().sorted shouldBe Seq(1, 2, 3)
    dstIds.collect().sorted shouldBe Seq(1, 2, 3)
  }

  test("tag dangling vertices") {
    execTestTagDanglingVertices(
      Seq(1, 2, 3),
      Seq(1, 2, 3),
      Seq(
        (1, false),
        (2, false),
        (3, false)
      )
    )

    execTestTagDanglingVertices(
      Seq(2, 3),
      Seq(1, 2, 3),
      Seq(
        (1, true),
        (2, false),
        (3, false)
      )
    )

    execTestTagDanglingVertices(
      Seq(1, 2, 3),
      Seq(2, 3),
      Seq(
        (1, false),
        (2, false),
        (3, false)
      )
    )

    execTestTagDanglingVertices(
      Seq(2, 3, 5, 6),
      Seq(1, 2, 3, 4, 5),
      Seq(
        (1, true),
        (2, false),
        (3, false),
        (4, true),
        (5, false),
        (6, false)
      )
    )
  }

  test("count dangling vertices") {
    val fixtures = Seq(
      (
        Seq(
          (1, 2, 1.0),
          (3, 2, 1.0),
          (2, 1, 1.0),
          (2, 3, 1.0)
        ),
        0
      ),
      (
        Seq(
          (2, 1, 1.0),
          (3, 1, 1.0),
          (4, 2, 1.0),
          (4, 3, 1.0),
          (5, 3, 1.0),
          (5, 4, 1.0)
          // 1 has no outgoing edges
          // 5 has no incoming edges
        ),
        1
      ),
      (
        Seq(
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
        ),
        2
      )
    )

    fixtures.foreach { case (input, expected) =>
      val actual = GraphUtils.countDanglingVertices(input)
      actual shouldBe expected
    }
  }

  test("count self-references") {
    val fixtures = Seq(
      (
        Seq(
          (1, 3, 1.0),
          (3, 1, 1.0),
          (4, 2, 1.0),
          (4, 3, 1.0),
          (5, 3, 1.0),
          (5, 4, 1.0)
        ),
        0
      ),
      (
        Seq(
          (1, 1, 1.0),
          (3, 1, 1.0),
          (4, 2, 1.0),
          (4, 3, 1.0),
          (5, 3, 1.0),
          (5, 4, 1.0)
        ),
        1
      ),
      (
        Seq(
          (1, 1, 1.0),
          (3, 1, 1.0),
          (4, 2, 1.0),
          (4, 4, 1.0),
          (5, 3, 1.0),
          (5, 4, 1.0)
        ),
        2
      )
    )

    fixtures.foreach { case (input, expected) =>
      val actual = GraphUtils.countSelfReferences(input)
      actual shouldBe expected
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

    fixtures.foreach { case (input, expected) =>
      val rdd = sc.parallelize(input.zipWithIndex.map(x => Vertex(x._2, x._1)))
      val actual = GraphUtils.areVerticesNormalized(rdd)
      actual shouldBe expected
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

    fixtures.foreach { case (input, expected) =>
      val actual = GraphUtils.countVerticesWithoutNormalizedOutEdges(input)
      actual shouldBe expected
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

    fixtures.foreach { case (input, expected) =>
      val rdd = GraphUtils.normalizeOutEdgeWeights(input)
      val actual = edgeSeqToTupleSeq(rdd.collect())
      actual.sorted shouldBe expected.sorted
    }
  }

  test("validate graph structure; no errors") {
    val errorsOpt = GraphUtils.validateStructure(Seq(Edge(1, 2, 1.0)), Seq(Vertex(1, 1.0)))
    errorsOpt.isEmpty shouldBe true
  }

  test("validate graph structure; one error") {
    val errorsOpt = GraphUtils.validateStructure(Seq(Edge(1, 1, 1.0)), Seq(Vertex(1, 1.0)))
    errorsOpt.isEmpty shouldBe false
    val errors = errorsOpt.get
    println(errors.mkString("\n"))
    errors.size shouldBe 1
    errors.head.contains("self-referencing") shouldBe true
  }

  test("validate graph structure; two errors") {
    val errorsOpt = GraphUtils.validateStructure(Seq(Edge(1, 1, 1.0)), Seq.empty[Vertex])
    errorsOpt.isEmpty shouldBe false
    val errors = errorsOpt.get
    println(errors.mkString("\n"))
    errors.size shouldBe 2
    errors(0).contains("self-referencing") shouldBe true
    errors(1).contains("values must be normalized") shouldBe true
  }

  private def execTestTagDanglingVertices(srcIds: Seq[Id], dstIds: Seq[Id], expected: Seq[(Id, Boolean)]): Unit = {
    val actual = GraphUtils.tagDanglingVertices(sc.parallelize(srcIds), sc.parallelize(dstIds))
    actual.collect().sortBy(_._1) shouldBe expected
  }
}
