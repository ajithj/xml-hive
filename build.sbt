import AssemblyKeys._
name := "xml-hive"

version := "0.1"

scalaVersion := "2.10.4"

resolvers ++= Seq(
      "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
      "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
      "Sonatype Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
      "Sonatype Releases" at "http://oss.sonatype.org/content/repositories/releases",
      "Conjars" at "http://conjars.org/repo",
      "Maven Central" at "http://repo2.maven.org/maven2/"
    )

libraryDependencies ++= Seq(
"org.scalatest" % "scalatest_2.10" % "2.2.4" % "test",
"org.apache.hive" % "hive-serde" % "2.1.0" % "provided",
//"org.scala-lang" % "scala-xml" % "2.11.0-M4",
"org.apache.hadoop" % "hadoop-client" % "2.7.2" % "provided",
"org.apache.avro" % "avro" % "1.7.7" % "provided",
"com.fasterxml.jackson.core" % "jackson-core" % "2.6.3",
"log4j" % "log4j" % "1.2.17" % "provided",
"org.apache.logging.log4j" % "log4j-api" % "2.2" % "provided"
)

assemblySettings


//scalaVersion := "2.11.7"