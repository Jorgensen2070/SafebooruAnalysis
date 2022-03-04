
name := "SafebooruSpark"

version := "0.1"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
/*assemblyJarName in assembly := "InitialAnalysis.jar"
mainClass in assembly := Some("InitialAnalysis")*/

assemblyJarName in assembly := "UpdateAnalysis.jar"
mainClass in assembly := Some("UpdateAnalysis")

libraryDependencies ++=Seq(
  "org.mongodb.scala" %% "mongo-scala-driver" % "4.1.2",
  "org.mongodb.spark" %% "mongo-spark-connector" % "3.0.1",
  "org.apache.spark" %% "spark-core" % "3.0.1",
  "org.apache.spark" %% "spark-sql" % "3.1.2",
  "org.scalatest" %% "scalatest" % "3.1.1" % "test",
  "org.scalactic" %% "scalactic" % "3.1.1",
)