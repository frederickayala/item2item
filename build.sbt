name := """flinkitem2item"""

version := "1.0"

//resolvers ++= Seq("Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/")
//resolvers += "mvnrepository" at "http://mvnrepository.com/artifact/"
//resolvers += "central" at "http://repo1.maven.org/maven2/"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.1.7" % "test"

libraryDependencies  ++= Seq(
  "org.scalanlp" %% "breeze" % "0.11.2",
  "org.scalanlp" %% "breeze-natives" % "0.11.2",
  "org.scalanlp" %% "breeze-viz" % "0.11.2"
)

libraryDependencies += "com.typesafe.scala-logging" % "scala-logging_2.11" % "3.1.0"
//libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.10"
//libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.2"

libraryDependencies ++= Seq("org.apache.flink" % "flink-scala_2.11" % "0.10.2",
							"org.apache.flink" % "flink-streaming-scala_2.11" % "0.10.2",
							"org.apache.flink" % "flink-clients_2.11" % "0.10.2"
//              "org.apache.flink" % "flink-ml" % "0.10.0" exclude("org.scalanlp", "breeze_${scala.binary.version}")
							)

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.4.3",
  "com.typesafe.akka" %% "akka-agent" % "2.4.3",
  "com.typesafe.akka" %% "akka-slf4j" % "2.4.3"
)

resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "1.3.0"

libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.3.0"
libraryDependencies += "org.scalaj" % "scalaj-http_2.11" % "2.3.0"

scalaVersion := "2.11.7"

fork in run := true

// set the main class for 'sbt run'
mainClass in (Compile, run) := Some("item2item.EuclideanItemRecommenderTrainer")

mainClass in (Compile, packageBin) := Some("item2item.EuclideanItemRecommenderTrainer")

scalacOptions ++= Seq("-Xmax-classfile-name","255")

packAutoSettings