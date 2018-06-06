resolvers in ThisBuild ++= Seq(
    "Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/",
    Resolver.mavenLocal
)

name := "dpasf"

version := "0.1-SNAPSHOT"

organization := "com.elbauldelprogramador"

scalaVersion in ThisBuild := "2.11.3"

val flinkVersion = "1.4.0"

val genericDeps = Seq(
//  "com.google.guava" % "guava" % "24.0-jre"
  "nz.ac.waikato.cms.moa" % "moa" % "2017.06"
)

val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided")

val testDeps = Seq(
  "org.scalatest" %% "scalatest" % "3.0.4" % "test",
  "org.scalacheck" %% "scalacheck" % "1.13.4" % "test")

val loggers = Seq(
//  "org.log4s" %% "log4s" % "1.3.4",
  "ch.qos.logback" % "logback-classic" % "1.2.3")

lazy val root = (project in file(".")).
  settings(
    libraryDependencies ++= flinkDependencies ++ testDeps ++ genericDeps ++ loggers
  )

mainClass in assembly := Some("com.elbauldelprogramador.Job")

// make run command include the provided dependencies
run in Compile := Defaults.runTask(fullClasspath in Compile,
                                   mainClass in (Compile, run),
                                   runner in (Compile,run)
                                  ).evaluated

// exclude Scala library from assembly
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
