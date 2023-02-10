import Dependencies._

ThisBuild / scalaVersion     := "2.13.10"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1")

lazy val root = (project in file("."))
  .settings(
    name := "reproducing-kf-bug-ce-3",
    libraryDependencies ++= Seq(
      KafkaFlow.core,
      ApacheCommons.lang3
    ),
    dependencyOverrides ++= Seq(
      Overrides.scalaJava8Compat
    ),
    excludeDependencies ++= Seq(
      ExclusionRule(organization = "com.evolutiongaming", name = "scache")
    )
  )

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
