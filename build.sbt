
lazy val root = (project in file(".")).
  settings(
    name := "LocalCluster",
    version := "0.1",
    scalaVersion := "2.11.8",
    test in assembly := {}

    //mainClass in Compile := Some("myPackage.MyMainObject")
  ).settings(
    assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
  ).settings(
    fork in Test := true,
    parallelExecution in Test := false
  )

javaOptions ++= Seq("-Xms2G", "-Xmx8G", "-XX:+CMSClassUnloadingEnabled")

spDependencies +=  "RedisLabs/spark-redis:0.3.2"


libraryDependencies ++= Seq(
  "com.github.scopt" % "scopt_2.11" % "3.5.0",
  "com.github.nikita-volkov" % "sext" % "0.2.6",
  "ch.qos.logback" % "logback-classic" % "1.1.8",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
  "org.apache.commons" % "commons-io" % "1.3.2",

  "redis.clients" % "jedis" % "2.7.2",

  "org.apache.spark" % "spark-core_2.11" % "2.0.1" % "provided",
  "org.apache.spark" % "spark-graphx_2.11" % "2.0.1" % "provided",
  "org.apache.spark" % "spark-mllib_2.11" % "2.0.1" % "provided",
  "javax.servlet" % "servlet-api" % "2.5",

  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "2.7.3" % "provided",
  "org.apache.hadoop" % "hadoop-common" % "2.7.3" % "provided",

  "org.slf4j" % "slf4j-simple" % "1.7.21" % "provided",
  "org.slf4j" % "slf4j-api" % "1.7.21" % "provided",


  "org.scalatest" % "scalatest_2.11" % "2.2.2" % "provided",
  "com.holdenkarau" % "spark-testing-base_2.11" % "2.0.1_0.6.0" % "provided",
  "com.lordofthejars" % "nosqlunit-redis" % "0.14.0"  % "provided"

)


assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs@_*) => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith ".html" => MergeStrategy.first
  case "application.conf" => MergeStrategy.concat
  case "unwanted.txt" => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
