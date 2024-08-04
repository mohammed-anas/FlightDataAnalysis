name := "Flight_Data_Analytics"
version := "0.1"
scalaVersion := "2.12.10"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.8",
  "org.apache.spark" %% "spark-sql" % "2.4.8"
)

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.15" % Test
