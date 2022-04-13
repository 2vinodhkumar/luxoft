ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.12"

libraryDependencies ++= sparkDependecies
lazy val root = (project in file("."))
  .settings(
    name := "SensorStats"
  )
val sparkCore="org.apache.spark" %% "spark-core" %  "2.3.0"
val sparkStreaming = "org.apache.spark" %% "spark-streaming" %  "2.3.0"
val sparkSql= "org.apache.spark" %% "spark-sql" %  "2.3.0"
val sparkDependecies= Seq(sparkCore,sparkStreaming,sparkSql)


