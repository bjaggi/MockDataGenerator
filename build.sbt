name := "MilvusDataGenerator"
version := "1.0"
scalaVersion := "2.11.8"
// With Park 2.2 above was spark 1.6
val sparkVersion = "2.2.0"
val twitterForsparkVersion = "1.6.1"
resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,


  "org.scalactic" %% "scalactic" % "3.0.4",
  "org.scalatest" %% "scalatest" % "3.0.4" % "test",
  "junit" % "junit" % "4.12",
  "com.holdenkarau" %% "spark-testing-base" % "1.6.1_0.3.2"


)


libraryDependencies += "joda-time" % "joda-time" % "2.9.9"

