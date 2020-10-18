name := "du-general-files"

version := "0.1"

scalaVersion := "2.12.7"

resourceDirectory in Compile := baseDirectory.value / "resources"

libraryDependencies += "junit" % "junit" % "4.12"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"
// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.7.3"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % Test
libraryDependencies += "org.specs2" %% "specs2-core" % "3.8.6" % Test
//libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"
//libraryDependencies += "com.typesafe" % "config" % "1.2.1"
libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.4.0"
)

