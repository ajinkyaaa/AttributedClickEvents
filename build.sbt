name := "test"

version := "0.1"

scalaVersion := "2.11.8"

parallelExecution in Test := false
concurrentRestrictions in Global += Tags.limit(Tags.Test, 1)
Test / parallelExecution := false
parallelExecution in ThisBuild := false
libraryDependencies ++= Seq(
  // https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.11
  "org.apache.spark" %% "spark-core" % "2.3.2" ,
  // https://mvnrepository.com/artifact/org.apache.spark/spark-sql_2.11
  "org.apache.spark" %% "spark-sql" % "2.3.2" ,
  "org.scalactic" %% "scalactic" % "3.0.8",
  "org.scalatest" %% "scalatest" % "3.0.8" % "test")