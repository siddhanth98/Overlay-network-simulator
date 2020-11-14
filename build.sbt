lazy val akkaVersion = "2.6.10"
lazy val akkaHttpVersion = "10.2.1"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaVersion % Test,
  "org.scalatest" %% "scalatest" % "3.1.0" % Test,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-cluster-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-serialization-jackson" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster-sharding-typed" % akkaVersion,
  "com.google.guava" % "guava-primitives" % "r03",
  "com.google.guava" % "guava" % "30.0-jre",
  "com.amihaiemil.web" % "eo-yaml" % "5.1.6",
  "org.yaml" % "snakeyaml" % "1.9"
)
