import sbt._

object Dependencies {

  object KafkaFlow {
    private val version = "2.2.12"
    val core = "com.evolutiongaming" %% "kafka-flow" % version
  }

  object ApacheCommons {
    val lang3 = "org.apache.commons" % "commons-lang3" % "3.12.0"
  }

  object Overrides {
    val scalaJava8Compat = "org.scala-lang.modules" %% "scala-java8-compat" % "1.0.2"
  }
}
