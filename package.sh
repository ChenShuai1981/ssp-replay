#!/bin/bash

# Build project source
cd /opt/
bash ./sbt -Dsbt.repository.config=repositories -Dsbt.override.build.repos=true -Dsbt.log.noformat=true clean assembly || exit $?

# Create symbolic link for output folder
ln -s /opt/target/scala-* /opt/target/scala
