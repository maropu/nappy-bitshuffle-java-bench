name := "snappy-bitshuffle-bench"

version := "0.0.1"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.6.0",
  "org.apache.parquet" % "parquet-column" % "1.7.0",
  "org.apache.commons" % "commons-lang3" % "3.0",
  "org.scalatest" % "scalatest_2.10" % "2.2.4"
)
