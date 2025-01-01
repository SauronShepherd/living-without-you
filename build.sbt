organization := "spark-wtf"
name := "living-without-you"
version := "1.0-SNAPSHOT"

scalaVersion := "2.12.17"

lazy val sparkVersion = "3.5.4"
lazy val graphframesVersion = "0.8.1-spark3.0-s_2.12"

resolvers ++= Seq(
  "Spark Packages Repo" at "https://repos.spark-packages.org/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-graphx" % sparkVersion,
  "graphframes" % "graphframes" % graphframesVersion

)
