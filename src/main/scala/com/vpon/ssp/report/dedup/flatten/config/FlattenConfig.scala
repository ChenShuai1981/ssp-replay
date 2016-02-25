package com.vpon.ssp.report.dedup.flatten.config

import scala.concurrent.duration.FiniteDuration

import com.vpon.ssp.report.dedup.couchbase.RxCouchbaseBucket

case class FlattenConfig (
  bannerDelayPeriod: FiniteDuration,
  interstitialDelayPeriod: FiniteDuration,
  bucket: RxCouchbaseBucket,
  bucketKeyPrefix: String,
  couchbaseMaxRetries: Int,
  couchbaseRetryInterval: FiniteDuration,
  secretKeyMap: Map[String, AnyRef],
  concurrencyLevel: Int,
  placementInitialCapacity: Int,
  placementMaxSize: Int,
  placementExpire: FiniteDuration,
  publisherInitialCapacity: Int,
  publisherMaxSize: Int,
  publisherExpire: FiniteDuration,
  exchangeRateInitialCapacity: Int,
  exchangeRateMaxSize: Int,
  exchangeRateExpire: FiniteDuration,
  publisherSspTaxRateInitialCapacity: Int,
  publisherSspTaxRateMaxSize: Int,
  publisherSspTaxRateExpire: FiniteDuration,
  dspSspTaxRateInitialCapacity: Int,
  dspSspTaxRateMaxSize: Int,
  dspSspTaxRateExpire: FiniteDuration,
  deviceInitialCapacity: Int,
  deviceMaxSize: Int,
  geographyInitialCapacity: Int,
  geographyMaxSize: Int)
