import org.apache.spark.graphx.{ Edge, Graph }
import org.apache.spark.storage.StorageLevel

import com.soundcloud.spark.PageRank

val inputPath = "/user/josh/discorank/normalizedEdges"
val outputPath = "/user/josh/discorank/prVector"

val edges = sc.objectFile[Edge[Double]](inputPath)
val numVertices = 650284017
val prior = 1.0 / numVertices
val graph = Graph.fromEdges(
  edges,
  defaultValue = prior,
  vertexStorageLevel = StorageLevel.MEMORY_AND_DISK,
  edgeStorageLevel = StorageLevel.MEMORY_AND_DISK
)

val pr = PageRank.run(
  graph,
  teleportProb = 0.15,
  maxIterations = 2,
  convergenceThreshold = None
)

pr.saveAsObjectFile(outputPath)
