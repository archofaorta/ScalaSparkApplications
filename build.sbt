name := "ScalaSparkApplications"

version := "0.1"

scalaVersion := "2.12.8"


val sparkVersion = "2.2.4"

/*resolvers ++= Seq(
  ("apache-snapshots" at "http://repository.apache.org/snapshots/").withAllowInsecureProtocol(true)
)*/

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.4",
  "org.apache.spark" %% "spark-sql" % "2.4.4",
  "org.apache.spark" %% "spark-mllib" % "2.4.4" % "runtime",
  "org.apache.spark" %% "spark-streaming" % "2.4.4" % "provided",
  "org.apache.spark" %% "spark-hive" % "2.4.4" % "provided",
  "org.apache.spark" %% "spark-catalyst" % "2.4.4" % Test,
  "org.apache.spark" %% "spark-graphx" % "2.4.4",
  "org.apache.spark" %% "spark-repl" % "2.4.4" % "provided",
  /*"org.apache.spark" %% "spark-streaming-kafka" % "1.6.3"*/

)


