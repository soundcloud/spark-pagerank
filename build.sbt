organization := "com.soundcloud"

name := "spark-pagerank"

scalaVersion := "2.11.8"

crossScalaVersions := Seq("2.10.4", "2.11.8")

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
  "args4j" % "args4j" % "2.0.29",
  "org.apache.spark" %% "spark-core"   % "2.1.0" % "provided"
)

// test dependencies
libraryDependencies ++= Seq(
  "org.scalatest"  %% "scalatest" % "2.2.4" % "test"
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
