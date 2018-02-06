resolvers in ThisBuild ++= Seq(
    "Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/",
    Resolver.mavenLocal
)

name := "dpasf"

version := "0.1-SNAPSHOT"

organization := "com.elbauldelprogramador"

scalaVersion in ThisBuild := "2.11.7"

val flinkVersion = "1.4.0"

val genericDeps = Seq(
  "com.google.guava" % "guava" % "24.0-jre")

val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided")

val testDeps = Seq(
  "org.scalatest" %% "scalatest" % "3.0.4" % "test",
  "org.scalacheck" %% "scalacheck" % "1.13.4" % "test")

lazy val root = (project in file(".")).
  settings(
    libraryDependencies ++= flinkDependencies ++ testDeps ++ genericDeps
  )

mainClass in assembly := Some("com.elbauldelprogramador.Job")

// make run command include the provided dependencies
run in Compile := Defaults.runTask(fullClasspath in Compile,
                                   mainClass in (Compile, run),
                                   runner in (Compile,run)
                                  ).evaluated

// exclude Scala library from assembly
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
