organization := "io.mola"

name := "checksum-spark"

version := "0.1.0"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.1" % Provided

libraryDependencies += "com.github.scopt" %% "scopt" % "3.7.0"

libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "2.2.1_0.9.0" % Test

fork in Test := true

javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")

parallelExecution in Test := false

assemblyJarName in assembly := "spark-checksum-fat.jar"

test in assembly := {}

mainClass in assembly := Some("io.mola.spark.checksum.App")

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

artifact in (Compile, assembly) := {
  val art = (artifact in (Compile, assembly)).value
  art.withClassifier(Some("assembly"))
}

addArtifact(artifact in (Compile, assembly), assembly)
