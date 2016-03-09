package com.vpon.ssp.report.replay

import com.typesafe.config.ConfigFactory

trait ReplayConfig {

  val config = ConfigFactory.load()

  lazy val regionName = config.getString("ssp-replay.archive.s3.region-name")
  lazy val archiveS3BucketName = config.getString("ssp-replay.archive.s3.bucket-name")
  lazy val archivePeriod = config.getString("ssp-replay.archive.period")
  lazy val archiveKafkaTopic = config.getString("ssp-replay.archive.kafka-topic")
  lazy val kafkaReplayBrokers = config.getString("ssp-replay.replay.kafka.brokers")
  lazy val kafkaReplayTopic = config.getString("ssp-replay.replay.kafka.topic")
  lazy val kafkaReplayKeySerializer = config.getString("ssp-replay.replay.kafka.serializer.key")
  lazy val kafkaReplayValueSerializer = config.getString("ssp-replay.replay.kafka.serializer.value")
  lazy val cbConnectionString = config.getString("ssp-replay.replay.couchbase.connection-string")
  lazy val cbBucketName = config.getString("ssp-replay.replay.couchbase.bucket.name")
  lazy val cbBucketPassword = config.getString("ssp-replay.replay.couchbase.bucket.password")
  lazy val cbBucketKeyPrefix = config.getString("ssp-replay.replay.couchbase.bucket.key-prefix")

}
