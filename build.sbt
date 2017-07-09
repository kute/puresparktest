organization := "com.kute"

name := "puresparktest"

version := "1.0"

scalaVersion := "2.11.11"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.1.1",
  "org.apache.spark" % "spark-streaming_2.11" % "2.1.1",
  "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.1.1",
  "org.apache.spark" % "spark-streaming-flume_2.11" % "2.1.1",
  "org.apache.spark" % "spark-sql_2.11" % "2.1.1",
  "com.typesafe.akka" % "akka-actor_2.11" % "2.5.1",
  "mysql" % "mysql-connector-java" % "6.0.5",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
  "org.hammerlab" % "spark-tests_2.11" % "2.0.0" % "test"
)

scalacOptions ++= Seq("-deprecation", "-unchecked")
    