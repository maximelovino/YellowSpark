name := "YellowSpark"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq("io.spray" %% "spray-json" % "1.3.4",
  "org.apache.spark" %% "spark-core" % "2.4.3",
  "org.apache.spark" %% "spark-sql" % "2.4.3",
  "org.apache.spark" %% "spark-mllib" % "2.4.3",
  "com.esri.geometry" % "esri-geometry-api" % "1.0"
)