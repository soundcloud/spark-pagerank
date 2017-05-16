package com.soundcloud.spark.pagerank

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.kohsuke.args4j.{ CmdLineParser, Option }

/**
 * Builds a PageRank graph from (non-normalized) weighted edges, saving the
 * results in a Parquet file for next steps.
 */
object GraphBuilderApp extends SparkApp {
  class Options {
    @Option(name = "--input", usage = "Input directory containing edges in TSV format (source, destination, weight)", required = true)
    var input: String = _

    @Option(name = "--output", usage = "Root output directory for the built graph (edges and vertices)", required = true)
    var output: String = _

    @Option(name = "--validateGraphStructure", usage = "Validate the structural properties of the graph, according to the requirements of PageRank, printing any errors to stdout")
    var validateGraphStructure: Boolean = false

    @Option(name = "--numPartitions", usage = "Number of partitions to use at work time and for the resulting output")
    var numPartitions: Int = 4000
  }

  def run(args: Array[String], spark: SparkSession): Unit = {
    val options = new Options()
    new CmdLineParser(options).parseArgument(args: _*)

    runFromInputs(options, spark, spark.sparkContext.textFile(options.input, minPartitions = options.numPartitions))
  }

  /**
   * An integration testable run interface.
   */
  private[pagerank] def runFromInputs(options: Options, spark: SparkSession, input: RDD[String]): Unit = {
    // read TSV edges
    // coalesce to smaller number of partitions
    // convert to internal edges data type
    val weightedEdges = input
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
    PageRankGraph.save(spark, graph, options.output)

    // run additional graph statistics (optional)
    // TODO(jd): does not exist yet

    // run graph validation (optional)
    if (options.validateGraphStructure) {
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
