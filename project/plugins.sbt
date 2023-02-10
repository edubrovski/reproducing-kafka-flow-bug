externalResolvers := Seq(
  Resolver.url(
    "Evolution Gaming",
    url("https://rms.evolutiongaming.com/pub-ivy/")
  )(Resolver.ivyStylePatterns),
  "Evolution Gaming repository" at "https://rms.evolutiongaming.com/public/"
)

addSbtPlugin("com.evolution" % "sbt-artifactory-plugin" % "0.0.2")

addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.2.1")
