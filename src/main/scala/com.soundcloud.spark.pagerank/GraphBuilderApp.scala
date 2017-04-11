package com.soundcloud.spark.pagerank

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.kohsuke.args4j.{ CmdLineParser, Option }

/**
 * Builds a PageRank graph from (non-normalized) weighted edges.
 */
object GraphBuilderApp extends SparkApp {
  class Options {
    @Option(name = "--input", usage = "Input directory containing edges in TSV format (source, destination, weight)", required = true)
    var input: String = _

    @Option(name = "--output", usage = "Root output directory for the built graph (edges and vertices)", required = true)
    var output: String = _

    // @Option(name = "--computeExtraGraphStats", usage = "Compute and save extra graph statistics")
    // var extractGraphStats: Boolean = false

    @Option(name = "--validateGraph", usage = "Validate the structural properties of the graph, according to the requirements of PageRank, printing any errors to stdout")
    var validateGraph: Boolean = false

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
      .repartition(options.numPartitions)
      .map(parseEdge)
      .persist(StorageLevel.MEMORY_ONLY_2)

    // normalize edges
    val edges = GraphUtils.normalizeOutEdgeWeights(weightedEdges)

    // unpersist temporary datasets
    weightedEdges.unpersist()

    // build PageRank graph
    val graph = PageRankGraph.fromEdgesWithUniformPriors(
      edges,
      tmpStorageLevel = StorageLevel.MEMORY_ONLY_2,
      edgesStorageLevel = StorageLevel.MEMORY_AND_DISK,
      verticesStorageLevel = StorageLevel.MEMORY_AND_DISK
    )

    // save graph
    PageRankGraph.save(graph, options.output)

    // run additional graph statistics (optional)
    // TODO(jd): does not exist yet

    // run graph validation (optional)
    if (options.validateGraph) {
      graph.validateStructure().foreach { errors =>
        println(errors.mkString("\n"))
      }
    }
  }

  private[pagerank] def parseEdge(str: String): Edge = {
    val Array(srcId, dstId, weight) = str.split("\t")
    Edge(srcId.toLong, dstId.toLong, weight.toDouble)
  }
}
