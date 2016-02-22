package com.vpon.ssp.report.dedup.config

import java.util.concurrent.TimeUnit

import scala.language.postfixOps
import scala.concurrent.duration._
import scala.collection.JavaConversions._
import scala.util.Try

import spray.json.{JsNumber, JsString, JsObject, JsValue}
import com.couchbase.client.java.CouchbaseCluster
import com.typesafe.config.ConfigFactory

import com.vpon.ssp.report.common.config.{KafkaConfig, ClusterConfig, CouchbaseConfig}
import com.vpon.ssp.report.common.couchbase.BucketInfo


trait DedupConfig {

  val config = ConfigFactory.load()

  lazy val clusterGroup = config.getString("ssp-dedup.cluster-group")
  lazy val clusterRampupTime = config.getDuration("ssp-dedup.cluster-rampup-time", TimeUnit.SECONDS).seconds

  lazy val consumeBatchSize = config.getInt("ssp-dedup.consume-batch-size")
  lazy val dedupKeyTTL = config.getDuration("ssp-dedup.dedup-key-ttl", SECONDS).seconds

  /**
   * Retries
   */
  lazy val dedupMaxRetries = config.getInt("ssp-dedup.retries.dedup.max")
  lazy val dedupRetryInterval = config.getDuration("ssp-dedup.retries.dedup.interval", TimeUnit.MILLISECONDS).milliseconds

  lazy val flattenMaxRetries = config.getInt("ssp-dedup.retries.flatten.max")
  lazy val flattenRetryInterval = config.getDuration("ssp-dedup.retries.flatten.interval", TimeUnit.MILLISECONDS).milliseconds

  lazy val produceMaxRetries = config.getInt("ssp-dedup.retries.produce.max")
  lazy val produceRetryInterval = config.getDuration("ssp-dedup.retries.produce.interval", TimeUnit.MILLISECONDS).milliseconds

  lazy val couchbaseMaxRetries = config.getInt("ssp-dedup.retries.couchbase.max")
  lazy val couchbaseRetryInterval = config.getDuration("ssp-dedup.retries.couchbase.interval", MILLISECONDS).milliseconds

  /**
   * Flatten
   */
  lazy val bannerDelayPeriod = config.getDuration("ssp-dedup.flatten.delay-period.banner", SECONDS).seconds
  lazy val interstitialDelayPeriod = config.getDuration("ssp-dedup.flatten.delay-period.interstitial", SECONDS).seconds

  lazy val secretKeyMap = config.getObject("ssp-dedup.flatten.secret-key").unwrapped().toMap
  lazy val concurrencyLevel = config.getInt("ssp-dedup.flatten.cache.concurrency-level")

  lazy val placementInitialCapacity = config.getInt("ssp-dedup.flatten.cache.placement.initial-capacity")
  lazy val publisherInitialCapacity = config.getInt("ssp-dedup.flatten.cache.publisher.initial-capacity")
  lazy val exchangeRateInitialCapacity = config.getInt("ssp-dedup.flatten.cache.exchange-rate.initial-capacity")
  lazy val publisherSspTaxRateInitialCapacity = config.getInt("ssp-dedup.flatten.cache.publisher-ssp-tax-rate.initial-capacity")
  lazy val dspSspTaxRateInitialCapacity = config.getInt("ssp-dedup.flatten.cache.dsp-ssp-tax-rate.initial-capacity")
  lazy val deviceInitialCapacity = config.getInt("ssp-dedup.flatten.cache.device.initial-capacity")
  lazy val geographyInitialCapacity = config.getInt("ssp-dedup.flatten.cache.geography.initial-capacity")

  lazy val placementMaxSize = config.getInt("ssp-dedup.flatten.cache.placement.max-size")
  lazy val publisherMaxSize = config.getInt("ssp-dedup.flatten.cache.publisher.max-size")
  lazy val exchangeRateMaxSize = config.getInt("ssp-dedup.flatten.cache.exchange-rate.max-size")
  lazy val publisherSspTaxRateMaxSize = config.getInt("ssp-dedup.flatten.cache.publisher-ssp-tax-rate.max-size")
  lazy val dspSspTaxRateMaxSize = config.getInt("ssp-dedup.flatten.cache.dsp-ssp-tax-rate.max-size")
  lazy val deviceMaxSize = config.getInt("ssp-dedup.flatten.cache.device.max-size")
  lazy val geographyMaxSize = config.getInt("ssp-dedup.flatten.cache.geography.max-size")

  lazy val placementExpire = config.getDuration("ssp-dedup.flatten.cache.placement.expire", SECONDS).seconds
  lazy val publisherExpire = config.getDuration("ssp-dedup.flatten.cache.publisher.expire", SECONDS).seconds
  lazy val exchangeRateExpire = config.getDuration("ssp-dedup.flatten.cache.exchange-rate.expire", SECONDS).seconds
  lazy val publisherSspTaxRateExpire = config.getDuration("ssp-dedup.flatten.cache.publisher-ssp-tax-rate.expire", SECONDS).seconds
  lazy val dspSspTaxRateExpire = config.getDuration("ssp-dedup.flatten.cache.dsp-ssp-tax-rate.expire", SECONDS).seconds

  /**
   * Kafka
   */
  lazy val sourceTopic = config.getString("kafka.consumer.topic")
  lazy val sourceBrokers = config.getString("kafka.consumer.brokers")

  lazy val delayedEventsTopic = config.getString("kafka.producer.delayed.topic")
  lazy val delayedEventsBrokers = config.getString("kafka.producer.delayed.brokers")

  lazy val warningEventsTopic = config.getString("kafka.producer.warning.topic")
  lazy val warningEventsBrokers = config.getString("kafka.producer.warning.brokers")

  lazy val flattenEventsTopic = config.getString("kafka.producer.flatten.topic")
  lazy val flattenEventsBrokers = config.getString("kafka.producer.flatten.brokers")

  /**
   * Couchbase
   */
  lazy val couchbaseConnectionString = config.getString("couchbase.connection-string")
  lazy val couchbaseCluster = CouchbaseCluster.fromConnectionString(couchbaseConnectionString)
  lazy val bucketsMap = config.getObject("couchbase.buckets").toMap map {
    case (key, value) =>
      val bucketConfig = value.asInstanceOf[com.typesafe.config.ConfigObject].toConfig
      val name = bucketConfig.getString("name")
      val password = bucketConfig.getString("password")
      val keyPrefix = bucketConfig.getString("key-prefix")
      (key, BucketInfo(name, password, keyPrefix))
  }

  def config2Json(): JsValue = {
    JsObject(
      "ssp-dedup" -> JsObject(
        "cluster-group" -> JsString(clusterGroup),
        "cluster-rampup-time" -> JsString(clusterRampupTime.toSeconds + "s"),
        "consume-batch-size" -> JsNumber(consumeBatchSize),
        "dedup-key-ttl" -> JsString(dedupKeyTTL.toSeconds + "s"),
        "retries" -> JsObject(
          "dedup" -> JsObject(
            "max" -> JsNumber(dedupMaxRetries),
            "interval" -> JsString(dedupRetryInterval.toMillis + "ms")
          ),
          "flatten" -> JsObject(
            "max" -> JsNumber(flattenMaxRetries),
            "interval" -> JsString(flattenRetryInterval.toMillis + "ms")
          ),
          "produce" -> JsObject(
            "max" -> JsNumber(produceMaxRetries),
            "interval" -> JsString(produceRetryInterval.toMillis + "ms")
          ),
          "couchbase" -> JsObject(
            "max" -> JsNumber(couchbaseMaxRetries),
            "interval" -> JsString(couchbaseRetryInterval.toMillis + "ms")
          )
        ),
        "flatten" -> JsObject(
          "delay-period" -> JsObject(
            "banner" -> JsString(bannerDelayPeriod.toSeconds + "ms"),
            "interstitial" -> JsString(interstitialDelayPeriod.toSeconds + "ms")
          ),
          "secret-key" -> JsObject(
            secretKeyMap.map(
              m => m._1 -> JsString(m._2.toString)
            )
          ),
          "cache" -> JsObject(
            "concurrency-level" -> JsNumber(concurrencyLevel),
            "publisher" -> JsObject(
              "max-size" -> JsNumber(publisherMaxSize),
              "initial-capacity" -> JsNumber(publisherInitialCapacity),
              "expire" -> JsString(publisherExpire.toSeconds + "s")
            ),
            "placement" -> JsObject(
              "max-size" -> JsNumber(placementMaxSize),
              "initial-capacity" -> JsNumber(placementInitialCapacity),
              "expire" -> JsString(placementExpire.toSeconds + "s")
            ),
            "exchange-rate" -> JsObject(
              "max-size" -> JsNumber(exchangeRateMaxSize),
              "initial-capacity" -> JsNumber(exchangeRateInitialCapacity),
              "expire" -> JsString(exchangeRateExpire.toSeconds + "s")
            ),
            "publisher-ssp-tax-rate" -> JsObject(
              "max-size" -> JsNumber(publisherSspTaxRateMaxSize),
              "initial-capacity" -> JsNumber(publisherSspTaxRateInitialCapacity),
              "expire" -> JsString(publisherSspTaxRateExpire.toSeconds + "s")
            ),
            "dsp-ssp-tax-rate" -> JsObject(
              "max-size" -> JsNumber(dspSspTaxRateMaxSize),
              "initial-capacity" -> JsNumber(dspSspTaxRateInitialCapacity),
              "expire" -> JsString(dspSspTaxRateExpire.toSeconds + "s")
            ),
            "device" -> JsObject(
              "max-size" -> JsNumber(deviceMaxSize),
              "initial-capacity" -> JsNumber(deviceInitialCapacity)
            ),
            "geography" -> JsObject(
              "max-size" -> JsNumber(geographyMaxSize),
              "initial-capacity" -> JsNumber(geographyInitialCapacity)
            )
          )
        )
      ),
      "couchbase" -> JsObject(
        "connection-string" -> JsString(couchbaseConnectionString),
        "buckets" -> JsObject(
          bucketsMap.map(m =>
            m._1 -> JsObject(
              "name" -> JsString(m._2.name),
              "password" -> JsString(m._2.password),
              "key-prefix" -> JsString(m._2.keyPrefix)
            )
          )
        )
      ),
      "kafka" -> JsObject(
        "producer" -> JsObject(
          "delayed" -> JsObject(
            "topic" -> JsString(delayedEventsTopic),
            "brokers" -> JsString(delayedEventsBrokers)
          ),
          "warning" -> JsObject(
            "topic" -> JsString(warningEventsTopic),
            "brokers" -> JsString(warningEventsBrokers)
          ),
          "flatten" -> JsObject(
            "topic" -> JsString(flattenEventsTopic),
            "brokers" -> JsString(flattenEventsBrokers)
          )
        ),
        "consumer" -> JsObject(
          "topic" -> JsString(sourceTopic),
          "brokers" -> JsString(sourceBrokers)
        )
      )
    )
  }

}
