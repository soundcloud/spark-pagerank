organization := "com.soundcloud"

name := "spark-pagerank"

scalaVersion := "2.10.4"

scalacOptions ++= Seq(
  "-unchecked",
  "-deprecation",
  "-optimise",
  "-feature",
  "-language:implicitConversions"
)

// can't run multiple SparkContext's in local mode in parallel
parallelExecution in Test := false

// main dependencies
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.2.1" % "provided"
)

// test dependencies
libraryDependencies ++= Seq(
  "com.soundcloud" %% "spark-lib" % "0.2.0" % "test",
  "org.scalatest"  %% "scalatest" % "2.2.0" % "test"
)

resolvers ++= Seq(
  "SoundCloud Internal - Hosted Snapshots" at "http://maven.int.s-cloud.net/content/groups/hosted_snapshots/",
  "SoundCloud Internal - Hosted Releases"  at "http://maven.int.s-cloud.net/content/groups/hosted_releases/",
  "SoundCloud Internal - Proxy Snapshots"  at "http://maven.int.s-cloud.net/content/groups/proxy_snapshots/",
  "SoundCloud Internal - Proxy Releases"   at "http://maven.int.s-cloud.net/content/groups/proxy_releases/"
)

publishTo <<= version { v =>
  val nexus = "http://maven.int.s-cloud.net/content/repositories/"
  if (v.trim.endsWith("SNAPSHOT"))
    Some("snapshots" at nexus + "snapshots")
  else
    Some("releases" at nexus + "releases")
}

// dependency plugin
// docs: https://github.com/jrudolph/sbt-dependency-graph
net.virtualvoid.sbt.graph.Plugin.graphSettings

// release plugin
// docs: https://github.com/sbt/sbt-release
releaseSettings
