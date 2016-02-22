# Vpon SSP Dedup

De-duplicating AdEvent messages (Tradelog, Impression, Click) sent from SSP Edge and do flatten which convert message to analyzable event records.
Later SSP Indexing will post these analyzable event records into SSP Druid Service for OLAP analysis.

### clone this project

```bash
git clone git@192.168.100.5:adn/ssp-dedup.git
cd ssp-dedup
git checkout dev
```

### make configurations
- all settings in application.conf, e.g., ./env/dev/application.conf
- see logback.xml for log related config, e.g., ./env/dev/logback.xml

### load latest ssp-common-module files

```bash
~ % git submodule init
~ % git submodule update
~ % cd lib/ssp-common-module
~ % git pull origin dev
```

### config kafka

```bash
cd KAFKA_HOME
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 10 --topic ssp-edge-events
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 10 --topic ssp-flatten-events
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 10 --topic ssp-delayed-events
```

### config couchbase

Create the following buckets: ssp_dedup_offset, ssp_dedup_key, ssp_web_api_dev


### add some supporting data

```bash
sbt "runMain com.vpon.ssp.report.dedup.SupportingData -b <couchbaseNodes> -n <bucketName> -p <password> -k <keyPrefix>
for example, sbt "runMain com.vpon.ssp.report.dedup.SupportingData -b http://localhost:8091 -n ssp_web -p 123456 -k ssp_web_
```

### check supporting data was inserted into `ssp_web_api_dev` bucket
Use couchbase admin console and the total number of supporting data should be 26


### launch application

```bash
./bin/dev.sh
```

### launch different simulators to push event messages into kafka

```bash
1. sbt "runMain com.vpon.ssp.report.dedup.SingleEventSimulator"
2. sbt "runMain com.vpon.ssp.report.dedup.StreamEventsSimulator"
3. sbt "runMain com.vpon.ssp.report.dedup.TripleEventsSimulator"
```
SingleEventSimulator will emit single event message into kafka once you press `Enter` key
StreamEventsSimulator will ask you which number of messages you want to push
TripleEventsSimulator will emit triple event messages (TradeLog, Impression, Click) every time


### monitor with metrics

Open browser and hit http://localhost:12661/metrics


### change log level
localhost:12661/loglevel?level=<LOG_LEVEL>

LOG_LEVEL can be WARN, INFO, DEBUG, ERROR
e.g., localhost:12661/loglevel?level=DEBUG


### pause work on all partitions in current node
localhost:12661/pauseAll


### pause work on specified partition in current node
localhost:12661/pause?pid=<PARTITION_ID>
e.g., localhost:12661/pause?pid=2


### resume work on all partitions in current node
localhost:12661/resumeAll

### resume work on specified partition in current node
localhost:12661/resume?pid=<PARTITION_ID>
e.g., localhost:12661/resume?pid=2


### check topic `ssp-flatten-events` of kafka

Use kafkacat (https://github.com/edenhill/kafkacat) or kafka-console-consumer.sh to view flatten messages produced

```bash
cd kafkacat
./kafkacat -b localhost:9092 -t ssp-flatten-events
```
or

```bash
cd KAFKA_HOME
bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic ssp-flatten-events --from-beginning
```

### check couchbase bucket

ssp_dedup_key stores all dedup keys while ssp_dedup_offset stores offset of last sent message

### run unit test

Setup kafka topic and couchbase bucket as described in test/resources/application.conf
sbt test