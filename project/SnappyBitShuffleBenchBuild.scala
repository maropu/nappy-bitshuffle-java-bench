import sbt._
import sbt.Keys._

object SnappyBitShuffleBenchBuild extends Build {

  lazy val sparkcompat = Project(
    id = "snappy-bitshuffle-bench",
    base = file("."),
    settings = Project.defaultSettings ++ Seq(
      name := "snappy-bitshuffle-bench",
      organization := "maropu",
      version := "0.0.1",
      scalaVersion := "2.10.4"
      // add other settings here
    )
  )
}
