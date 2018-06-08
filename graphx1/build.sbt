name := "Simple Project"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "2.0.1",
    "org.apache.spark" %% "spark-sql" % "2.0.1",
    "org.apache.spark" %% "spark-mllib-local" % "2.0.1",
    "com.github.fommil.netlib" % "all" % "1.1.2",
    "org.apache.spark" %% "spark-mllib" % "2.0.1"
  )