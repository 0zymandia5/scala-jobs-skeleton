name := "sbtproject"

version := "0.1"

scalaVersion := "2.12.12"
showSuccess := false

libraryDependencies += "org.apache.bahir" %% "spark-sql-cloudant" % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.7"
libraryDependencies += "com.crealytics" %% "spark-excel" % "0.12.1"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.7"
libraryDependencies += "com.lihaoyi" %% "requests" % "0.1.9"

libraryDependencies += "io.github.cdimascio" % "java-dotenv" % "3.0.0"

mainClass := Some("Hello")
mainClass in (Compile, bgRun) := Some("Hello")
mainClass in (Compile, packageBin) := Some("Hello")