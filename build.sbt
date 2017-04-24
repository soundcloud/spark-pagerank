organization := "com.soundcloud"

name := "spark-pagerank"

scalaVersion := "2.11.8"

crossScalaVersions := Seq("2.10.4", "2.11.8")

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
  "args4j" % "args4j" % "2.0.29",
  "org.apache.spark" %% "spark-core"   % "2.1.0" % "provided"
)

// test dependencies
libraryDependencies ++= Seq(
  "org.scalatest"  %% "scalatest" % "2.2.4" % "test"
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

pomExtra := (
  <scm>
    <url>git@github.com:soundcloud/spark-pagerank.git</url>
    <connection>scm:git@github.com:soundcloud/spark-pagerank.git</connection>
  </scm>
  <developers>
    <developer>
      <id>joshdevins</id>
      <name>Josh Devins</name>
      <url>http://www.joshdevins.com</url>
      <email>hi@joshdevins.com</email>
    </developer>
  </developers>
)

// dependency plugin
// docs: https://github.com/jrudolph/sbt-dependency-graph
net.virtualvoid.sbt.graph.Plugin.graphSettings
