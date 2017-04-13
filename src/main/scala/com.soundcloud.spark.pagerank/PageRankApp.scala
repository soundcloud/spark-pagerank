package com.soundcloud.spark.pagerank

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.kohsuke.args4j.{ CmdLineParser, Option => ArgOption }

/**
 * Runs PageRank on the graph produced by {{GraphBuilderApp}}.
 */
object PageRankApp extends SparkApp {
  class Options {
    @ArgOption(name = "--input", usage = "Root input directory containing the built graph (edges and vertices)", required = true)
    var input: String = _

    @ArgOption(name = "--output", usage = "Output directory for the final PageRank vertices", required = true)
    var output: String = _

    @ArgOption(name = "--priors", usage = "Directory with priors to use, allowing PageRank to continue iterating after stopping, requiring the identical graph that was used to produce the priors")
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

  def run(args: Array[String], sc: SparkContext): Unit = {
    val options = new Options()
    new CmdLineParser(options).parseArgument(args: _*)

    sc.setCheckpointDir(s"${options.output}__checkpoints")

    runFromInputs(
      options,
      sc,
      sc.textFile(s"${options.input}/stats").collect(),
      sc.objectFile[OutEdgePair](s"${options.input}/edges"),
      sc.objectFile[RichVertexPair](s"${options.input}/vertices"),
      options.priorsOpt.map(x => sc.objectFile[Vertex](s"$x"))
    )
  }

  /**
   * An integration testable run interface.
   */
  private[pagerank] def runFromInputs(
      options: Options,
      sc: SparkContext,
      stats: Seq[String],
      edges: OutEdgePairRDD,
      vertices: RichVertexPairRDD,
      priorsOpt: Option[VertexRDD]): Unit = {

    val verticesForGraph = priorsOpt match {
      case None => vertices
      case Some(priors) => replaceValuesWithPriors(vertices, priors)
    }

    // reassemble the PageRankGraph from it's constituent components
    val graph = PageRankGraph(
      extractStatistic(stats, "numVertices")(_.toLong),
      edges.persist(StorageLevel.MEMORY_AND_DISK_2),
      verticesForGraph.persist(StorageLevel.MEMORY_AND_DISK_2)
    )

    val solution = PageRank.run(
      graph,
      teleportProb = options.teleportProb,
      maxIterations = options.maxIterations,
      convergenceThresholdOpt = options.convergenceThresholdOpt
    )

    solution.map(toTsv).saveAsObjectFile(options.output)
  }

  private[pagerank] def replaceValuesWithPriors(vertices: RichVertexPairRDD, priors: VertexRDD): RichVertexPairRDD = {
    val priorsPairs = priors.map(x => (x.id, x.value))
    vertices
      .join(priorsPairs)
      .map { case (id, (vMeta, prior)) =>
        (id, vMeta.withNewValue(prior))
      }
  }

  private[pagerank] def extractStatistic[T](stats: Seq[String], key: String)(parse: (String) => T): T = {
    stats.map(_.split(",")).find(_.apply(0) == key) match {
      case Some(pair) => parse(pair(1))
      case None => throw new IllegalArgumentException(s"Statistic not found with key: $key")
    }
  }

  private[pagerank] def toTsv(v: Vertex): String =
    s"${v.id}\t${v.value}"
}
