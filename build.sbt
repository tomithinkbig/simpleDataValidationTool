name := "dataValidationTool"

version := "0.1"

//scalaVersion := "2.11.7"
scalaVersion := "2.10.4"   // http://stackoverflow.com/questions/32189206/how-to-setup-intellij-14-scala-worksheet-to-run-spark

//TODO add more explicit dependencies to resolve compiler warnings about different versions of libraries
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.5.2" % "provided"
  , "org.apache.spark" %% "spark-hive" % "1.5.2" % "provided"
  , "org.json4s" %% "json4s-native" % "3.3.0"
  , "com.typesafe" % "config" % "1.3.0"
)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

mainClass in (Compile, packageBin) := Some("com.thinkbiganalytics.datavalidationtool.Main")
