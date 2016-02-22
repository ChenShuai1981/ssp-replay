#!/usr/bin/env bash
sbt "runMain com.vpon.ssp.report.dedup.simulator.StreamEventsSimulator -b localhost:9092 -t ssp-edge-events"
#java -cp ../target/scala-2.11/ssp-dedup-assembly-0.1.jar com.vpon.ssp.report.dedup.simulator.StreamEventsSimulator -b localhost:9092 -t ssp-edge-events