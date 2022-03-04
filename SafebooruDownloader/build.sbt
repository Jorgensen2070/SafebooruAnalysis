
name := "SafebooruDownloader"

version := "0.1"

scalaVersion := "2.12.13"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
assemblyJarName in assembly := "InitialDownload.jar"
mainClass in assembly := Some("InitialDownload")

/*assemblyJarName in assembly := "UpdateDownload.jar"
mainClass in assembly := Some("UpdateDownload")*/

val AkkaVersion = "2.6.8"
val AkkaHttpVersion = "10.2.4"
libraryDependencies ++=Seq(
  "org.mongodb.scala" %% "mongo-scala-driver" % "4.1.2",
  "org.mongodb.spark" %% "mongo-spark-connector" % "3.0.1",
  "org.apache.spark" %% "spark-core" % "3.0.1",
  "org.apache.spark" %% "spark-sql" % "3.0.1",
  "org.scalaj" %% "scalaj-http" % "2.4.2",
  "io.spray" %%  "spray-json" % "1.3.6"
)