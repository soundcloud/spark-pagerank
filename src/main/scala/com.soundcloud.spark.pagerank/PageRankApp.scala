package com.soundcloud.spark.pagerank

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.kohsuke.args4j.{CmdLineParser, Option => ArgOption}

/**
 * Runs PageRank on the graph produced by {{GraphBuilderApp}}.
 */
object PageRankApp extends SparkApp {
  class Options {
    @ArgOption(name = "--input", usage = "Root input directory containing the built graph (edges and vertices)", required = true)
    var input: String = _

    @ArgOption(name = "--output", usage = "Output directory for the final PageRank vertices", required = true)
    var output: String = _

    @ArgOption(name = "--priors", usage = "Directory with priors to use, allowing PageRank to continue iterating after stopping or starting off a new run from previous execution potentially with different vertices")
    var priors: String = _

    @ArgOption(name = "--teleportProb", usage = "PageRank: probability of a random jump in the graph")
    var teleportProb: Value = PageRank.DefaultTeleportProb

    @ArgOption(name = "--maxIterations", usage = "PageRank: a threshold on the maximum number of iterations, irrespective of convergence")
    var maxIterations: Int = PageRank.DefaultMaxIterations

    @ArgOption(name = "--convergenceThreshold", usage = "PageRank: an optional threshold on the change between iterations which marks convergence (NOTE: providing this will cause an extra computation after each iteration, so if performance is of concern, do not provide a value here)")
    var convergenceThreshold: Value = 0.0

    def priorsOpt: Option[String] = priors match {
      case null => None
      case x => Some(x)
    }

    def convergenceThresholdOpt: Option[Value] = convergenceThreshold match {
      case 0.0 => None
      case x => Some(x)
    }
  }

  def run(args: Array[String], spark: SparkSession): Unit = {
    val options = new Options()
    new CmdLineParser(options).parseArgument(args: _*)

    spark.sparkContext.setCheckpointDir(s"${options.output}__checkpoints")

    val graph = PageRankGraph.load(
      spark,
      options.input,
      edgesStorageLevel = StorageLevel.MEMORY_AND_DISK_2,
      verticesStorageLevel = StorageLevel.MEMORY_AND_DISK_2
    )

    runFromInputs(
      options,
      graph,
      options.priorsOpt.map(x => spark.sparkContext.objectFile[Vertex](s"$x"))
    )
  }

  /**
   * An integration testable run interface.
   */
  private[pagerank] def runFromInputs(
      options: Options,
      inputGraph: PageRankGraph,
      priorsOpt: Option[VertexRDD]): Unit = {

    // replace priors, if another vector was supplied
    val graph = priorsOpt match {
      case None => inputGraph
      case Some(priors) => inputGraph.updateVertexValues(priors)
    }

    PageRank.run(
      graph,
      teleportProb = options.teleportProb,
      maxIterations = options.maxIterations,
      convergenceThresholdOpt = options.convergenceThresholdOpt
    )
    .saveAsObjectFile(options.output)
  }

  private[pagerank] def extractStatistic[T](stats: Seq[String], key: String)(parse: (String) => T): T = {
    stats.map(_.split(",")).find(_.apply(0) == key) match {
      case Some(pair) => parse(pair(1))
      case None => throw new IllegalArgumentException(s"Statistic not found with key: $key")
    }
  }
}
