package com.soundcloud.spark.pagerank

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.kohsuke.args4j.{ CmdLineParser, Option }

object GraphBuilderApp extends SparkApp {
  class Options {
    @Option(name = "--input", usage = "Input directory containing edges in TSV format (source, destination, weight)", required = true)
    var input: String = _

    @Option(name = "--output", usage = "Root output directory for the built graph (edges and vertices)", required = true)
    var output: String = _

    @Option(name = "--computeExtraGraphStats", usage = "Compute and save extra graph statistics")
    var extractGraphStats: Boolean = false

    @Option(name = "--validateGraph", usage = "Validate the structural properties of the graph")
    var validateGaph: Boolean = false

    @Option(name = "--numPartitions", usage = "Number of partitions to use at work time and for the resulting output")
    var numPartitions: Int = 4000
  }

  def run(args: Array[String], sc: SparkContext): Unit = {
    val options = new Options()
    new CmdLineParser(options).parseArgument(args: _*)

    runFromInputs(options, sc, sc.textFile(options.input))
  }

  /**
   * An integration testable run interface.
   */
  private[pagerank] def runFromInputs(options: Options, sc: SparkContext, input: RDD[String]): Unit = {
    // read TSV edges
    // coalesce to smaller number of partitions
    // convert to internal edges data type
    val weightedEdges = input
      .coalesce(options.numPartitions)
      .map(parseEdge)
      .persist(StorageLevel.MEMORY_ONLY_2)

    // normalize edges
    val edges = GraphUtils.normalizeOutEdgeWeights(weightedEdges)

    // unpersist temporary datasets
    weightedEdges.unpersist()

    // validate normalized edges are within some tolerance
    // TODO(jd): this function does not exist yet

    // build PageRank graph
    // don't persist since we just save straight to HDFS
    // TODO(jd): change that when we add running validation and statistics
    val graph = PageRankGraph.uniformPriorsFromEdges(
      edges,
      tmpStorageLevel = StorageLevel.MEMORY_ONLY_2,
      edgesStorageLevel = StorageLevel.NONE,
      verticesStorageLevel = StorageLevel.NONE
    )

    // save graph components
    graph.edges.saveAsObjectFile(s"${options.output}/edges")
    graph.vertices.saveAsObjectFile(s"${options.output}/vertices")

    // run additional graph statistics (optional)
    // TODO(jd): does not exist yet
    val extraStatistics = Seq.empty[String]

    // run graph validation (optional)
    // TODO(jd): does not exist yet

    // save graph statistics as map in single partition
    val basicStatistics = Seq(s"numVertices,${graph.numVertices}")
    sc
      .parallelize(basicStatistics ++ extraStatistics)
      .repartition(1)
      .saveAsTextFile(s"${options.output}/stats")
  }

  private[pagerank] def parseEdge(str: String): Edge = {
    val Array(srcId, dstId, weight) = str.split("\t")
    Edge(srcId.toLong, dstId.toLong, weight.toDouble)
  }
}
