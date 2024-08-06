name := "Flight_Data_Analytics"
version := "0.1"
scalaVersion := "2.12.10"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.8" % "test",
  "org.apache.spark" %% "spark-sql" % "2.4.8" % "test",
  "org.scalatest" %% "scalatest" % "3.2.11" % "test",
  "org.scalatestplus" %% "scalatestplus-spark" % "3.2.11.0" % "test"
)

