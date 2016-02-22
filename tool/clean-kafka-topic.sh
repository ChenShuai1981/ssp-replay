#!/usr/bin/env bash

/Users/chenshuai/dev/kafka_2.10-0.8.2.1/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic ssp-warning-events
/Users/chenshuai/dev/kafka_2.10-0.8.2.1/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic ssp-delayed-events
/Users/chenshuai/dev/kafka_2.10-0.8.2.1/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic ssp-edge-events
/Users/chenshuai/dev/kafka_2.10-0.8.2.1/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic ssp-flatten-events
/Users/chenshuai/dev/kafka_2.10-0.8.2.1/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic ssp-warning-events
/Users/chenshuai/dev/kafka_2.10-0.8.2.1/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic ssp-delayed-events
/Users/chenshuai/dev/kafka_2.10-0.8.2.1/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic ssp-edge-events
/Users/chenshuai/dev/kafka_2.10-0.8.2.1/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic ssp-flatten-events
