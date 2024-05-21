import scala.collection.immutable.Seq

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.13"

lazy val root = (project in file("."))
  .settings(
    name := "kafka-consumer-lag",
    organization := "com.bill",
    assembly / assemblyJarName := "example.jar",
    libraryDependencies ++= Seq(
      "org.apache.kafka" % "kafka-clients" % "3.7.0",
      "org.http4s" %% "http4s-dsl" % "0.23.12",
      "org.http4s" %% "http4s-blaze-server" % "0.23.12",
      "org.http4s" %% "http4s-circe" % "0.23.12",
      "io.circe" %% "circe-generic" % "0.14.1",
      "io.circe" %% "circe-parser" % "0.14.1",
      "org.typelevel" %% "cats-effect" % "3.3.11",
      "ch.qos.logback" % "logback-classic" % "1.5.6",
      "org.http4s" %% "http4s-ember-server" % "0.23.12",
      "org.apache.kafka" % "kafka-clients" % "3.0.0",
"org.scalatest" %% "scalatest" % "3.2.9" % Test
    ),
    assembly / assemblyMergeStrategy := {
      case "module-info.class" => MergeStrategy.discard
      case PathList("META-INF", "versions", "9", "module-info.class") => MergeStrategy.discard
      case x => (assembly / assemblyMergeStrategy).value.apply(x)
    },
    addCompilerPlugin(("org.typelevel" %% "kind-projector" % "0.13.3").cross(CrossVersion.full)),
    addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1"),
    scalacOptions ++= Seq(
      "-Ywarn-macros:after",
      "-Wnonunit-statement", // Warn about discarded values in statement position
      "-Wunused:imports", // Enable unused import warnings
      "-Wconf:cat=unused-imports:s", // Silently ignore unused imports
      "-Wconf:msg=unused value of .*:s", // Discard "unused value" warnings for cats.effect.IO[Unit]
      "-Xmaxwarns",
      "9999" // max warnings displayed
    )
  )
