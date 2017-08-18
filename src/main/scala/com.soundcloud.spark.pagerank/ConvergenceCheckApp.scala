package com.soundcloud.spark.pagerank

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.kohsuke.args4j.{ CmdLineParser, Option => ArgOption }

/**
 * Compares two PageRank vectors and lets the user determine if there is
 * convergence by oututting the sum of the component-wise difference of the
 * vectors. Note that this is an optional tool that is mostly used for
 * debugging. If the user is concerned with iterating until convergence, the
 * user can specify the convergence threshold at runtime to PageRank.
 */
object ConvergenceCheckApp extends SparkApp {
  class Options {
    @ArgOption(name = "--inputA", usage = "Version A of the PageRank vector", required = true)
    var inputA: String = _

    @ArgOption(name = "--inputB", usage = "Version B of the PageRank vector", required = true)
    var inputB: String = _
  }

  def run(args: Array[String], spark: SparkSession): Unit = {
    import spark.implicits._

    def read(path: String): VertexRDD =
      spark.read.parquet(path).as[Vertex].rdd

    val options = new Options()
    new CmdLineParser(options).parseArgument(args: _*)

    val a = read(options.inputA)
    val b = read(options.inputB)

    val delta = sumOfDifferences(a, b)

    println("Sum of the vertices: ")
    println(f"  A: ${a.map(_.value).sum()}")
    println(f"  B: ${b.map(_.value).sum()}")
    println(f"Sum of component-wise differences: $delta%.15f")
  }

  private[pagerank] def sumOfDifferences(left: VertexRDD, right: VertexRDD): Value = {
    val leftPair = left.map(v => (v.id, v.value))
    val rightPair = right.map(v => (v.id, v.value))

    leftPair
      .join(rightPair)
      .map { case (_, (l, r)) =>
        math.abs(l - r)
      }
      .sum()
  }
}
