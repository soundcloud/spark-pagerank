organization := "com.soundcloud"

name := "spark-pagerank"

scalaVersion := "2.11.11"

crossScalaVersions := Seq("2.10.6", "2.11.11")

publishMavenStyle := true

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
  "args4j" % "args4j" % "2.0.31" % "optional",
  "org.apache.spark" %% "spark-core" % "2.2.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.2.1" % "provided"
)

// test dependencies
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.6" % "test"
)

// sbt-release settings
releasePublishArtifactsAction := PgpKeys.publishSigned.value
releaseCrossBuild := true

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases" at nexus + "service/local/staging/deploy/maven2")
}

publishArtifact in Test := false

useGpg := true

// metadata
licenses += ("MIT", url("http://opensource.org/licenses/MIT"))

homepage := Some(url("https://github.com/soundcloud/spark-pagerank"))

scmInfo := Some(
  ScmInfo(
    url("https://github.com/soundcloud/spark-pagerank"),
    "scm:git@github.com:soundcloud/spark-pagerank.git"
  )
)

developers := List(
  Developer(
    id    = "joshdevins",
    name  = "Josh Devins",
    email = "josh@soundcloud.com",
    url   = url("http://joshdevins.com")
  )
)

// dependency plugin
// docs: https://github.com/jrudolph/sbt-dependency-graph
net.virtualvoid.sbt.graph.Plugin.graphSettings
