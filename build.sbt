name := "kafka-producer"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies += "org.apache.kafka" %% "kafka" % "2.0.1" % Compile

assemblyJarName in assembly := "kafka-producer.jar"

mainClass in assembly := Some("com.snowflake.kafka.Producer")
