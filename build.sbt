name := "snappy-bitshuffle-bench"

version := "0.0.1"

scalaVersion := "2.10.4"

parallelExecution in Test := false

libraryDependencies ++= Seq(
  // "org.xerial.snappy" % "snappy-java" % "1.1.2.4" % "provided",
  "org.apache.spark" % "spark-core_2.10" % "1.6.0",
  "org.apache.parquet" % "parquet-column" % "1.7.0",
  "org.apache.commons" % "commons-lang3" % "3.0",
  "org.scalatest" % "scalatest_2.10" % "2.2.4"
)

mergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*) =>
    MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".properties" =>
    MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" =>
    MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".xml" =>
    MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".types" =>
    MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".class" =>
    MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".thrift" =>
    MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "VERSION" =>
    MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".a" =>
    MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".so" =>
    MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".jnilib" =>
    MergeStrategy.first
  case "application.conf" =>
    MergeStrategy.concat
  case "unwanted.txt" =>
    MergeStrategy.discard
  case x =>
    val oldStrategy = (mergeStrategy in assembly).value
    oldStrategy(x)
}

