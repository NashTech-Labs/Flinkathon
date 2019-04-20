ThisBuild / scalaVersion := "2.12.7"
ThisBuild / organization := "com.knoldus"

val flinkVersion = "1.7.2"
lazy val hello = (project in file("."))
  .settings(
    name := "Flinkathon",
    libraryDependencies += "org.apache.flink" %% "flink-scala" % flinkVersion,
    libraryDependencies += "org.apache.flink" %% "flink-streaming-scala" % flinkVersion,
    libraryDependencies += "org.apache.flink" %% "flink-connector-kafka" % flinkVersion
  )