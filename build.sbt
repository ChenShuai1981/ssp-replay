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
    "org.anarres.lzo"       %  "lzo-hadoop"                % "1.0.5",
    "org.anarres.lzo"       %  "lzo-core"                  % "1.0.5",
    "org.anarres.lzo"       %  "lzo-commons"               % "1.0.5",
    "org.apache.commons"    %  "commons-compress"          % "1.10"
  )
}

Revolver.settings


fork in run := true
