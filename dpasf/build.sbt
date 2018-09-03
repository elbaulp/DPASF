resolvers in ThisBuild ++= Seq(
  "Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/",
  Resolver.mavenLocal
)

name := "dpasf"

version := "0.1"

organization := "com.elbauldelprogramador"

scalaVersion in ThisBuild := "2.11.3"

fork in run := true

val flinkVersion = "1.6.0"

val genericDeps = Seq(
  "nz.ac.waikato.cms.weka" % "weka-stable" % "3.8.2" % "provided"
)

val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion % "compile",
  "org.apache.flink" %% "flink-ml" % flinkVersion % "compile",
  "org.apache.flink" %% "flink-streaming-scala"  % flinkVersion  % "compile"
)

val testDeps = Seq(
  "org.scalatest" %% "scalatest" % "3.0.4" % "test",
  "org.scalacheck" %% "scalacheck" % "1.13.4" % "test")

val loggers = Seq(
  "ch.qos.logback" % "logback-classic" % "1.2.3")

lazy val root = (project in file(".")).
  settings(
    libraryDependencies ++= flinkDependencies ++ testDeps ++ genericDeps ++ loggers,
    scalacOptions += "-Xlog-implicits")

mainClass in assembly := Some("com.elbauldelprogramador.Main")

// make run command include the provided dependencies
run in Compile := Defaults.runTask(fullClasspath in Compile,
  mainClass in (Compile, run),
  runner in (Compile,run)
).evaluated

test in assembly := {}

// exclude Scala library from assembly
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
// Jar Name
assemblyJarName in assembly := "dpasf.jar"
// Assembly strategy
assemblyMergeStrategy in assembly := {
//  case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
//  case PathList("weka", xs @ _*)        =>
//    println(s"EN WEKA para $xs")
//    MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case "application.conf"                            => MergeStrategy.concat
  case "log4j.properties"                            => MergeStrategy.last
  case "unwanted.txt"                                => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
