name := "SparkGraphX"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies ++= Seq(
    "org.apache.spark" % "spark-core_2.12" % "2.4.5",
    "org.apache.spark" % "spark-graphx_2.12" % "2.4.5"
)
