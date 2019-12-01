name := "beymani-spark"

organization := "org.beymani"

version := "1.0"

scalaVersion := "2.12.0"

scalacOptions := Seq("-unchecked", "-deprecation")

isSnapshot := true

libraryDependencies ++=Seq(
  "org.apache.spark" %% "spark-core" % "3.0.0-preview" % "provided",
  "org.apache.spark" %% "spark-streaming" % "3.0.0-preview" % "provided",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.0.0-preview", 
  "org.apache.commons" % "commons-lang3" % "3.0",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.3.3",
  "com.fasterxml.jackson.module" % "jackson-module-scala_2.12" % "2.9.4",
  "org.apache.lucene" % "lucene-core" % "7.1.0",
  "org.apache.lucene" % "lucene-analyzers-common" % "7.1.0",
  "junit" % "junit" % "4.7" % "test",
  "org.scalatest" % "scalatest_2.10" % "2.0" % "test",
  "org.chombo" %% "chombo-spark" % "1.0",
  "mawazo" %% "chombo" % "1.0",
  "mawazo" %% "beymani" % "1.0",
  "mawazo" %% "hoidla" % "1.0",
  "mawazo" %% "avenir" % "1.0",
  "gov.nist.math" % "jama" % "1.0.3"
)
