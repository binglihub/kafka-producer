name := "kafka-producer"

version := "0.2"

scalaVersion := "2.12.8"

resolvers ++= Seq(
  Resolver.sonatypeRepo("public"),
  "Confluent Maven Repo" at "http://packages.confluent.io/maven/"
)

libraryDependencies += "org.apache.kafka" %% "kafka" % "2.0.1" % Compile

libraryDependencies += "org.apache.avro" % "avro" % "1.8.1"

libraryDependencies += "io.confluent" % "kafka-avro-serializer" % "5.1.2"

assemblyJarName in assembly := "kafka-producer.jar"

mainClass in assembly := Some("com.binglihub.kafka.producer.Producer")
