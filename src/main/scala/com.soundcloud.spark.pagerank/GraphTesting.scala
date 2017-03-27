package com.soundcloud.spark

import scala.reflect.ClassTag

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{ Edge, Graph }
import org.apache.spark.rdd.RDD

/**
 * Functions of building graphs or parts of graphs from more basic components,
 * for the purposes of testing graphs.
 */
trait GraphTesting {

  def sc: SparkContext

  def graphFromEdges[VD: ClassTag, ED: ClassTag](edges: Seq[Edge[ED]], defaultVertexValue: VD = 1.0): Graph[VD, ED] =
    Graph.fromEdges[VD, ED](sc.parallelize(edges), defaultVertexValue)

  implicit def collapseEdges[ED](e: Seq[Edge[ED]]): Seq[(Int, Int, ED)] =
    e.map(edge => (edge.srcId.toInt, edge.dstId.toInt, edge.attr))

  implicit def collapseEdgesWithoutValue[ED](e: Seq[Edge[ED]]): Seq[(Int, Int)] =
    e.map(edge => (edge.srcId.toInt, edge.dstId.toInt))

  /**
   * Builds up a sequence of `Edge`s from simple tuples, with default edge
   * value.
   */
  def expandEdgeTuples[ED](e: Seq[(Int, Int)], defaultValue: ED): Seq[Edge[ED]] = {
    e.map { case (src, dst) =>
      Edge(src.toLong, dst.toLong, defaultValue)
    }
  }

  /**
   * Builds up a sequence of `Edge`s from simple tuples, with default edge
   * value of `1.0` (`Double`).
   */
  implicit def expandEdgeTuples(e: Seq[(Int, Int)]): Seq[Edge[Double]] =
    expandEdgeTuples(e, 1.0)

  /**
   * Builds up a sequence of `Edge`s from simple tuples, including edge values.
   */
  implicit def expandEdgeTuples[ED: ClassTag](e: Seq[(Int, Int, ED)]): Seq[Edge[ED]] = {
    e.map { case (src, dst, value) =>
      Edge(src.toLong, dst.toLong, value)
    }
  }
}
