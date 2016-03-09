name := """ssp-replay"""

organization  := "com.vpon"

version := "0.1-SNAPSHOT"

scalaVersion := "2.11.7"

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

libraryDependencies ++= {
  Seq(
    "com.typesafe"          %  "config"                    % "1.2.1",
    "com.amazonaws"         %  "aws-java-sdk"              % "1.10.57",
    "com.couchbase.client"  %  "java-client"               % "2.2.1",
    "commons-lang"          %  "commons-lang"              % "2.6",
    "org.apache.kafka"      %% "kafka"                     % "0.8.2.1",
    "com.github.scopt"      %% "scopt"                     % "3.3.0"
  )
}

Revolver.settings


fork in run := true
