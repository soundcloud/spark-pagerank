/**
cat ":load buildGraph.scala" | crun -i -o "--net=host -e HADOOP_USER_NAME=josh" hadoop-cdh5-spark-2.1.0 -- spark-shell \
  --master "yarn" \
  --deploy-mode "client" \
  --driver-memory 2G \
  --num-executors 64 \
  --executor-cores 8 \
  --executor-memory 16G \
  --conf "spark.hadoop.yarn.timeline-service.enabled=false" \
  --conf "spark.executorEnv.HADOOP_USER_NAME=$HADOOP_USER_NAME" \
  --conf "spark.dynamicAllocation.enabled=false" \
  --jars spark-pagerank_*.jar
 */

import org.apache.spark.graphx.Edge
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import com.soundcloud.spark.GraphUtils

def tsvToEdges(lines: RDD[String]): RDD[Edge[Double]] = {
  lines.map { line =>
    val Array(from, to, weight) = line.split("\t")
    Edge(from.toLong, to.toLong, weight.toDouble)
  }
}

val build = "2017-03-19T02-12-33"
val inputPath = s"/srv/discovery/search/ranking/discorank_v2/${build}/edges"
val baseOutputPath = "/user/josh/discorank"
val edgesPath = s"${baseOutputPath}/edges"
val verticesPath = s"${baseOutputPath}/vertices"
val statsPath = s"${baseOutputPath}/stats"

// load edges and get edge stats
val raw = sc.textFile(inputPath).coalesce(4096)
val edges = GraphUtils.
  normalizeOutEdgeWeightsRDD(tsvToEdges(raw)).
  persist(StorageLevel.MEMORY_AND_DISK)

// vertices...

// save RDDs
edges.saveAsObjectFile(edgesPath)
vertices.saveAsObjectFile(verticesPath)

val numEdges = edges.count()

// save stats
sc.
  parallelize(Seq(
    s"numVertices\t${numVertices}",
    s"numEdges\t${numEdges}"
  )).
  repartition(1).
  saveAsTextFile(statsPath)
