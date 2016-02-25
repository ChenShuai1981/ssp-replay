package com.vpon.ssp.report.dedup.actor

import java.lang.management.ManagementFactory
import javax.management.ObjectName

import akka.actor.{Actor, ActorLogging, Props}
import akka.util.Timeout

import spray.json._

import scala.concurrent.duration._
import scala.language.postfixOps

import com.google.common.cache.CacheStats

import com.vpon.mapping.{DeviceTypeMapping, GeographyMapping}
import com.vpon.ssp.report.dedup.actor.PartitionMetricsProtocol._
import com.vpon.ssp.report.flatten._

object PartitionMetricsProtocol {

  case object GetMetrics

  case object GetInfo

  case object Refresh

  case class Warning(warning: String)

  case class Error(error: String)

  case class Consume(offset: Long, key: String)

  case class SelfDedup(sourceCount: Int, targetCount: Int, time: Double)

  case class CouchbaseDedup(sourceCount: Int, targetCount: Int, time: Double)

  case class Flatten(time: Double)

  case class Send(time: Double)

  case class LastOffset(offset: Long)

  case class IsWorking(isWorking: Boolean)

  case object Reset

  case object Resume

  /**
   * FlattenFailures
   */

  case class CouchbaseError(offset: Long, key: String)

  case class UnknownError(offset: Long, key: String)

  case class CouchbaseDeserializationError(offset: Long, key: String)

  case class DelayedEvent(offset: Long, key: String)

  case class MappingError(offset: Long, key: String)

  case class UnParsedEvent(offset: Long, key: String)

  case class UnknownEventType(offset: Long, key: String)

  case class InvalidEvent(offset: Long, key: String)

  case class PlacementNotFound(offset: Long, key: String)

  case class PublisherNotFound(offset: Long, key: String)

  case class ExchangeRateNotFound(offset: Long, key: String)

  case class SecretKeyNotFound(offset: Long, key: String)

  case class DecryptClearPriceError(offset: Long, key: String)

  case class UnsupportedPublisherRevenueShareType(offset: Long, key: String)

  case class UnsupportedSellerRevenueShareType(offset: Long, key: String)

  case class UnsupportedDealType(offset: Long, key: String)

}

object PartitionMetrics {

  def akkaMBeansStatsPresentation(): JsObject = {
    val mbeanServer = ManagementFactory.getPlatformMBeanServer
    val clusterObjName = new ObjectName("akka:type=Cluster")
    JsObject("Available" -> JsString(mbeanServer.getAttribute(clusterObjName, "Available").toString),
      "ClusterStatus" -> JsonParser(mbeanServer.getAttribute(clusterObjName, "ClusterStatus").toString),
      "Leader" -> JsString(mbeanServer.getAttribute(clusterObjName, "Leader").toString),
      "MemberStatus" -> JsString(mbeanServer.getAttribute(clusterObjName, "MemberStatus").toString),
      "Members" -> JsString(mbeanServer.getAttribute(clusterObjName, "Members").toString),
      "Unreachable" -> JsString(mbeanServer.getAttribute(clusterObjName, "Unreachable").toString)
    )
  }

  def jvmMBeansStatsPresentation(): JsObject = {
    val osMXBean = ManagementFactory.getOperatingSystemMXBean
    val threadMXBean = ManagementFactory.getThreadMXBean
    val memoryMXBean = ManagementFactory.getMemoryMXBean

    JsObject("HeapMemoryUsage_init" -> JsNumber(memoryMXBean.getHeapMemoryUsage.getInit.toString),
      "HeapMemoryUsage_used" -> JsNumber(memoryMXBean.getHeapMemoryUsage.getUsed.toString),
      "NonHeapMemoryUsage_init" -> JsNumber(memoryMXBean.getNonHeapMemoryUsage.getInit.toString),
      "NonHeapMemoryUsage_used" -> JsNumber(memoryMXBean.getNonHeapMemoryUsage.getUsed.toString),
      "SystemLoadAverage" -> JsNumber(osMXBean.getSystemLoadAverage.toString),
      "PeakThreadCount" -> JsNumber(threadMXBean.getPeakThreadCount.toString),
      "ThreadCount" -> JsNumber(threadMXBean.getThreadCount.toString)
    )
  }

  def props(partitionId: Int): Props = {
    Props(new PartitionMetrics(partitionId))
  }

}

class PartitionMetrics(val partitionId: Int) extends Actor with ActorLogging {
  import PartitionMetrics._

  implicit val dispatcher = context.system.dispatcher

  implicit val timeout = Timeout(5 seconds)

  private var error = ""
  private var warning = ""

  private val newPercentage = 0.2
  private val oldPercentage = 1 - newPercentage

  private var totalFlattens = 0L
  private var avgFlattensPerSec = 0d
  private var accFlattens = 0L
  private var avgFlattenTime = 0d

  private var totalSelfDedups = 0L
  private var avgSelfDedupsPerSec = 0d
  private var accSelfDedups = 0L
  private var avgSelfDedupTime = 0d

  private var totalCouchbaseDedups = 0L
  private var avgCouchbaseDedupsPerSec = 0d
  private var accCouchbaseDedups = 0L
  private var avgCouchbaseDedupTime = 0d

  private var totalSends = 0L
  private var avgSendsPerSec = 0d
  private var accSends = 0L
  private var avgSendTime = 0d

  private var totalConsumes = 0L
  private var avgConsumesPerSec = 0d
  private var accConsumes = 0L

  private var lastOffset = 0L

  private var isWorking = true

  private var totalMappingErrors = 0L
  private var avgMappingErrorsPerSec = 0d
  private var accMappingErrors = 0L

  private var totalCouchbaseErrors = 0L
  private var avgCouchbaseErrorsPerSec = 0d
  private var accCouchbaseErrors = 0L

  private var totalUnknownErrors = 0L
  private var avgUnknownErrorsPerSec = 0d
  private var accUnknownErrors = 0L

  private var totalCouchbaseDeserializationErrors = 0L
  private var avgCouchbaseDeserializationErrorsPerSec = 0d
  private var accCouchbaseDeserializationErrors = 0L

  private var totalDelayEvents = 0L
  private var avgDelayEventsPerSec = 0d
  private var accDelayEvents = 0L

  private var totalUnParsedEvents = 0L
  private var avgUnParsedEventsPerSec = 0d
  private var accUnParsedEvents = 0L

  private var totalUnknownEventTypes = 0L
  private var avgUnknownEventTypesPerSec = 0d
  private var accUnknownEventTypes = 0L

  private var totalInvalidEvents = 0L
  private var avgInvalidEventsPerSec = 0d
  private var accInvalidEvents = 0L

  private var totalPlacementNotFounds = 0L
  private var avgPlacementNotFoundsPerSec = 0d
  private var accPlacementNotFounds = 0L

  private var totalPublisherNotFounds = 0L
  private var avgPublisherNotFoundsPerSec = 0d
  private var accPublisherNotFounds = 0L

  private var totalExchangeRateNotFounds = 0L
  private var avgExchangeRateNotFoundsPerSec = 0d
  private var accExchangeRateNotFounds = 0L

  private var totalSecretKeyNotFounds = 0L
  private var avgSecretKeyNotFoundsPerSec = 0d
  private var accSecretKeyNotFounds = 0L

  private var totalDecryptClearPriceErrors = 0L
  private var avgDecryptClearPriceErrorsPerSec = 0d
  private var accDecryptClearPriceErrors = 0L

  private var totalUnsupportedPublisherRevenueShareTypes = 0L
  private var avgUnsupportedPublisherRevenueShareTypesPerSec = 0d
  private var accUnsupportedPublisherRevenueShareTypes = 0L

  private var totalUnsupportedSellerRevenueShareTypes = 0L
  private var avgUnsupportedSellerRevenueShareTypesPerSec = 0d
  private var accUnsupportedSellerRevenueShareTypes = 0L

  private var totalUnsupportedDealTypes = 0L
  private var avgUnsupportedDealTypesPerSec = 0d
  private var accUnsupportedDealTypes = 0L

  private var lastCalAvgTimestamp = System.currentTimeMillis

  log.debug(s"${self.path} ==> PartitionMetrics is up")

  context.system.scheduler.schedule(0.second, 1.second, new Runnable {
    override def run() {
      self ! Refresh
    }
  })

  def receive: akka.actor.Actor.Receive = {

    case Resume =>
      log.debug(s"${self.path} ===> Resume")
      error = ""
      warning = ""
      isWorking = true

    case Reset =>
      log.debug(s"${self.path} ===> Reset")

      totalConsumes = 0L
      avgConsumesPerSec = 0d
      accConsumes = 0L

      totalSelfDedups = 0L
      avgSelfDedupsPerSec = 0d
      accSelfDedups = 0L
      avgSelfDedupTime = 0d

      totalCouchbaseDedups = 0L
      avgCouchbaseDedupsPerSec = 0d
      accCouchbaseDedups = 0L
      avgCouchbaseDedupTime = 0d

      totalFlattens = 0L
      avgFlattensPerSec = 0d
      accFlattens = 0L
      avgFlattenTime = 0d

      totalMappingErrors = 0L
      avgMappingErrorsPerSec = 0d
      accMappingErrors = 0L

      totalCouchbaseErrors = 0L
      avgCouchbaseErrorsPerSec = 0d
      accCouchbaseErrors = 0L

      totalUnknownErrors = 0L
      avgUnknownErrorsPerSec = 0d
      accUnknownErrors = 0L

      totalCouchbaseDeserializationErrors = 0L
      avgCouchbaseDeserializationErrorsPerSec = 0d
      accCouchbaseDeserializationErrors = 0L

      totalDelayEvents = 0L
      avgDelayEventsPerSec = 0d
      accDelayEvents = 0L

      totalUnParsedEvents = 0L
      avgUnParsedEventsPerSec = 0d
      accUnParsedEvents = 0L

      totalUnknownEventTypes = 0L
      avgUnknownEventTypesPerSec = 0d
      accUnknownEventTypes = 0L

      totalInvalidEvents = 0L
      avgInvalidEventsPerSec = 0d
      accInvalidEvents = 0L

      totalPlacementNotFounds = 0L
      avgPlacementNotFoundsPerSec = 0d
      accPlacementNotFounds = 0L

      totalPublisherNotFounds = 0L
      avgPublisherNotFoundsPerSec = 0d
      accPublisherNotFounds = 0L

      totalExchangeRateNotFounds = 0L
      avgExchangeRateNotFoundsPerSec = 0d
      accExchangeRateNotFounds = 0L

      totalSecretKeyNotFounds = 0L
      avgSecretKeyNotFoundsPerSec = 0d
      accSecretKeyNotFounds = 0L

      totalDecryptClearPriceErrors = 0L
      avgDecryptClearPriceErrorsPerSec = 0d
      accDecryptClearPriceErrors = 0L

      totalUnsupportedPublisherRevenueShareTypes = 0L
      avgUnsupportedPublisherRevenueShareTypesPerSec = 0d
      accUnsupportedPublisherRevenueShareTypes = 0L

      totalUnsupportedSellerRevenueShareTypes = 0L
      avgUnsupportedSellerRevenueShareTypesPerSec = 0d
      accUnsupportedSellerRevenueShareTypes = 0L

      totalUnsupportedDealTypes = 0L
      avgUnsupportedDealTypesPerSec = 0d
      accUnsupportedDealTypes = 0L

      totalSends = 0L
      avgSendsPerSec = 0d
      accSends = 0L
      avgSendTime = 0d

      lastCalAvgTimestamp = System.currentTimeMillis

      lastOffset = 0L
      error = ""
      warning = ""
      isWorking = true

    case Consume(offset, key) =>
      log.debug(s"${self.path} ===> Consume (offset = $offset, key = $key)")
      totalConsumes += 1
      calculateAverage(1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

    case SelfDedup(sourceCount, targetCount, time) =>
      log.debug(s"${self.path} ===> SelfDedup (sourceCount=$sourceCount, targetCount=$targetCount, time = $time)")
      totalSelfDedups += sourceCount
      avgSelfDedupTime = movingAverage(avgSelfDedupTime, time / sourceCount)
      calculateAverage(0, sourceCount, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

    case CouchbaseDedup(sourceCount, targetCount, time) =>
      log.debug(s"${self.path} ===> CouchbaseDedup (sourceCount=$sourceCount, targetCount=$targetCount, time = $time)")
      totalCouchbaseDedups += sourceCount
      avgCouchbaseDedupTime = movingAverage(avgCouchbaseDedupTime, time / sourceCount)
      calculateAverage(0, 0, sourceCount, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

    case Flatten(time) =>
      log.debug(s"${self.path} ===> Flatten (time = $time)")
      totalFlattens += 1
      avgFlattenTime = movingAverage(avgFlattenTime, time)
      calculateAverage(0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

    case MappingError(offset, key) =>
      log.debug(s"${self.path} ===> MappingError (offset = $offset, key = $key)")
      totalMappingErrors += 1
      calculateAverage(0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

    case CouchbaseError(offset, key) =>
      log.debug(s"${self.path} ===> CouchbaseError (offset = $offset, key = $key)")
      totalCouchbaseErrors += 1
      calculateAverage(0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

    case UnknownError(offset, key) =>
      log.debug(s"${self.path} ===> UnknownError (offset = $offset, key = $key)")
      totalUnknownErrors += 1
      calculateAverage(0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

    case CouchbaseDeserializationError(offset, key) =>
      log.debug(s"${self.path} ===> CouchbaseDeserializationError (offset = $offset, key = $key)")
      totalCouchbaseDeserializationErrors += 1
      calculateAverage(0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

    case DelayedEvent(offset, key) =>
      log.debug(s"${self.path} ===> DelayedEvent (offset = $offset, key = $key)")
      totalDelayEvents += 1
      calculateAverage(0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

    case UnParsedEvent(offset, key) =>
      log.debug(s"${self.path} ===> UnParsedEvent (offset = $offset, key = $key)")
      totalUnParsedEvents += 1
      calculateAverage(0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

    case UnknownEventType(offset, key) =>
      log.debug(s"${self.path} ===> UnknownEventType (offset = $offset, key = $key)")
      totalUnknownEventTypes += 1
      calculateAverage(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

    case InvalidEvent(offset, key) =>
      log.debug(s"${self.path} ===> InvalidEvent (offset = $offset, key = $key)")
      totalInvalidEvents += 1
      calculateAverage(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

    case PlacementNotFound(offset, key) =>
      log.debug(s"${self.path} ===> PlacementNotFound (offset = $offset, key = $key)")
      totalPlacementNotFounds += 1
      calculateAverage(0, 0, 0, 0, 0, 0,0,  0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

    case PublisherNotFound(offset, key) =>
      log.debug(s"${self.path} ===> PublisherNotFound (offset = $offset, key = $key)")
      totalPublisherNotFounds += 1
      calculateAverage(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0)

    case ExchangeRateNotFound(offset, key) =>
      log.debug(s"${self.path} ===> ExchangeRateNotFound (offset = $offset, key = $key)")
      totalExchangeRateNotFounds += 1
      calculateAverage(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0)

    case SecretKeyNotFound(offset, key) =>
      log.debug(s"${self.path} ===> SecretKeyNotFound (offset = $offset, key = $key)")
      totalSecretKeyNotFounds += 1
      calculateAverage(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0)

    case DecryptClearPriceError(offset, key) =>
      log.debug(s"${self.path} ===> DecryptClearPriceError (offset = $offset, key = $key)")
      totalDecryptClearPriceErrors += 1
      calculateAverage(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0)

    case UnsupportedPublisherRevenueShareType(offset, key) =>
      log.debug(s"${self.path} ===> UnsupportedPublisherRevenueShareType (offset = $offset, key = $key)")
      totalUnsupportedPublisherRevenueShareTypes += 1
      calculateAverage(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0)

    case UnsupportedSellerRevenueShareType(offset, key) =>
      log.debug(s"${self.path} ===> UnsupportedSellerRevenueShareType (offset = $offset, key = $key)")
      totalUnsupportedSellerRevenueShareTypes += 1
      calculateAverage(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0)

    case UnsupportedDealType(offset, key) =>
      log.debug(s"${self.path} ===> UnsupportedDealType (offset = $offset, key = $key)")
      totalUnsupportedDealTypes += 1
      calculateAverage(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0)

    case Send(time) =>
      log.debug(s"${self.path} ===> Send (time = $time)")
      totalSends += 1
      avgSendTime = movingAverage(avgSendTime, time)
      calculateAverage(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1)

    case Refresh =>
      calculateAverage(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

    case Error(err: String) if (!err.isEmpty) =>
      log.debug(s"${self.path} ===> Error")
      error = "%d => %s".format(System.currentTimeMillis, err)

    case Warning(warn: String) if (!warn.isEmpty) =>
      log.debug(s"${self.path} ===> Warning")
      warning = "%d => %s".format(System.currentTimeMillis, warn)

    case LastOffset(offset) =>
      log.debug(s"${self.path} ===> LastOffset (offset = $offset)")
      lastOffset = offset

    case IsWorking(working) =>
      log.debug(s"${self.path} ===> IsWorking ($working)")
      isWorking = working

    case GetMetrics =>
      log.debug(s"${self.path} ===> GetMetrics")
      sender() ! metricsStatsPresentation()

    case GetInfo =>
      log.debug(s"${self.path} ===> GetInfo")
      sender() ! infoStatsPresentation()
  }

  private def metricsStatsPresentation() = {
    JsObject(
      "partitionId" -> JsNumber(partitionId),
      "error" -> JsString(error),
      "warning" -> JsString(warning),
      "is_working" -> JsBoolean(isWorking),
      "last_offset" -> JsNumber(lastOffset),
      "total" -> JsObject(
        "consumes" -> JsNumber(totalConsumes),
        "selfDedups" -> JsNumber(totalSelfDedups),
        "couchbaseDedups" -> JsNumber(totalCouchbaseDedups),
        "flattens" -> JsNumber(totalFlattens),
        "mappingErrors" -> JsNumber(totalMappingErrors),
        "couchbaseErrors" -> JsNumber(totalCouchbaseErrors),
        "unknownErrors" -> JsNumber(totalUnknownErrors),
        "couchbaseDeserializationErrors" -> JsNumber(totalCouchbaseDeserializationErrors),
        "delayEvents" -> JsNumber(totalDelayEvents),
        "unParsedEvents" -> JsNumber(totalUnParsedEvents),
        "unknownEventTypes" -> JsNumber(totalUnknownEventTypes),
        "invalidEvents" -> JsNumber(totalInvalidEvents),
        "placementNotFounds" -> JsNumber(totalPlacementNotFounds),
        "publisherNotFounds" -> JsNumber(totalPublisherNotFounds),
        "exchangeRateNotFounds" -> JsNumber(totalExchangeRateNotFounds),
        "secretKeyNotFound" -> JsNumber(totalSecretKeyNotFounds),
        "decryptClearPriceErrors" -> JsNumber(totalDecryptClearPriceErrors),
        "unsupportedPublisherRevenueShareTypes" -> JsNumber(totalUnsupportedPublisherRevenueShareTypes),
        "unsupportedSellerRevenueShareTypes" -> JsNumber(totalUnsupportedSellerRevenueShareTypes),
        "unsupportedDealTypes" -> JsNumber(totalUnsupportedDealTypes),
        "sends" -> JsNumber(totalSends)
      ),
      "average" -> JsObject(
        "avg_self_dedup_time" -> JsNumber(avgSelfDedupTime),
        "avg_couchbase_dedup_time" -> JsNumber(avgCouchbaseDedupTime),
        "avg_flatten_time" -> JsNumber(avgFlattenTime),
        "avg_send_time" -> JsNumber(avgSendTime),
        "consumes_per_second" -> JsNumber(avgConsumesPerSec),
        "self_dedups_per_second" -> JsNumber(avgSelfDedupsPerSec),
        "couchbase_dedups_per_second" -> JsNumber(avgCouchbaseDedupsPerSec),
        "flattens_per_second" -> JsNumber(avgFlattensPerSec),
        "mapping_errors_per_second" -> JsNumber(avgMappingErrorsPerSec),
        "couchbase_errors_per_second" -> JsNumber(avgCouchbaseErrorsPerSec),
        "unknown_errors_per_second" -> JsNumber(avgUnknownErrorsPerSec),
        "couchbase_deserialization_errors_per_second" -> JsNumber(avgCouchbaseDeserializationErrorsPerSec),
        "delay_events_per_second" -> JsNumber(avgDelayEventsPerSec),
        "unparsed_events_per_second" -> JsNumber(avgUnParsedEventsPerSec),
        "unknown_event_types_per_second" -> JsNumber(avgUnknownEventTypesPerSec),
        "invalid_events_per_second" -> JsNumber(avgInvalidEventsPerSec),
        "placement_not_founds_per_second" -> JsNumber(avgPlacementNotFoundsPerSec),
        "publisher_not_founds_per_second" -> JsNumber(avgPublisherNotFoundsPerSec),
        "exchange_rate_not_founds_per_second" -> JsNumber(avgExchangeRateNotFoundsPerSec),
        "secret_key_not_founds_per_second" -> JsNumber(avgSecretKeyNotFoundsPerSec),
        "decrypt_clear_price_errors_per_second" -> JsNumber(avgDecryptClearPriceErrorsPerSec),
        "unsupported_publisher_revenue_share_types_per_second" -> JsNumber(avgUnsupportedPublisherRevenueShareTypesPerSec),
        "unsupported_seller_revenue_share_types_per_second" -> JsNumber(avgUnsupportedSellerRevenueShareTypesPerSec),
        "unsupported_deal_types_per_second" -> JsNumber(avgUnsupportedDealTypesPerSec),
        "send_per_second" -> JsNumber(avgSendsPerSec)
      )
    )
  }

  private def calculateAverage(
    consumeIncrementor: Int,
    selfDedupIncrementor: Int,
    couchbaseDedupIncrementor: Int,
    flattenIncrementor: Int,
    mappingErrorIncrementor: Int,
    couchbaseErrorIncrementor: Int,
    unknownErrorIncrementor: Int,
    couchbaseDeserializationErrorIncrementor: Int,
    delayedEventIncrementor: Int,
    unParsedEventIncrementor: Int,
    unknownEventTypeIncrementor: Int,
    invalidEventIncrementor: Int,
    placementNotFoundIncrementor: Int,
    publisherNotFoundIncrementor: Int,
    exchangeRateNotFoundIncrementor: Int,
    publisherSspTaxRateNotFoundIncrementor: Int,
    dspSspTaxRateNotFoundIncrementor: Int,
    secretKeyNotFoundIncrementor: Int,
    decryptClearPriceErrorIncrementor: Int,
    unsupportedPublisherRevenueShareTypeIncrementor: Int,
    unsupportedSellerRevenueShareTypeIncrementor: Int,
    unsupportedDealTypeIncrementor: Int,
    sendIncrementor: Int) = {

    val now = System.currentTimeMillis
    val timeDiffInSec = (now - lastCalAvgTimestamp) / 1000.0
    if (timeDiffInSec > 1) {
      lastCalAvgTimestamp = now
      avgConsumesPerSec = movingAverage(avgConsumesPerSec, accConsumes / timeDiffInSec)
      avgSelfDedupsPerSec = movingAverage(avgSelfDedupsPerSec, accSelfDedups / timeDiffInSec)
      avgCouchbaseDedupsPerSec = movingAverage(avgCouchbaseDedupsPerSec, accCouchbaseDedups / timeDiffInSec)
      avgFlattensPerSec = movingAverage(avgFlattensPerSec, accFlattens / timeDiffInSec)
      avgMappingErrorsPerSec = movingAverage(avgMappingErrorsPerSec, accMappingErrors / timeDiffInSec)
      avgCouchbaseErrorsPerSec = movingAverage(avgCouchbaseErrorsPerSec, accCouchbaseErrors / timeDiffInSec)
      avgUnknownErrorsPerSec = movingAverage(avgUnknownErrorsPerSec, accUnknownErrors / timeDiffInSec)
      avgCouchbaseDeserializationErrorsPerSec = movingAverage(avgCouchbaseDeserializationErrorsPerSec, accCouchbaseDeserializationErrors / timeDiffInSec)
      avgDelayEventsPerSec = movingAverage(avgDelayEventsPerSec, accDelayEvents / timeDiffInSec)
      avgUnParsedEventsPerSec = movingAverage(avgUnParsedEventsPerSec, accUnParsedEvents / timeDiffInSec)
      avgUnknownEventTypesPerSec = movingAverage(avgUnknownEventTypesPerSec, accUnknownEventTypes / timeDiffInSec)
      avgInvalidEventsPerSec = movingAverage(avgInvalidEventsPerSec, accInvalidEvents / timeDiffInSec)
      avgPlacementNotFoundsPerSec = movingAverage(avgPlacementNotFoundsPerSec, accPlacementNotFounds / timeDiffInSec)
      avgPublisherNotFoundsPerSec = movingAverage(avgPublisherNotFoundsPerSec, accPublisherNotFounds / timeDiffInSec)
      avgExchangeRateNotFoundsPerSec = movingAverage(avgExchangeRateNotFoundsPerSec, accExchangeRateNotFounds / timeDiffInSec)
      avgSecretKeyNotFoundsPerSec = movingAverage(avgSecretKeyNotFoundsPerSec, accSecretKeyNotFounds / timeDiffInSec)
      avgDecryptClearPriceErrorsPerSec = movingAverage(avgDecryptClearPriceErrorsPerSec, accDecryptClearPriceErrors / timeDiffInSec)
      avgUnsupportedPublisherRevenueShareTypesPerSec = movingAverage(avgUnsupportedPublisherRevenueShareTypesPerSec, accUnsupportedPublisherRevenueShareTypes / timeDiffInSec)
      avgUnsupportedSellerRevenueShareTypesPerSec = movingAverage(avgUnsupportedSellerRevenueShareTypesPerSec, accUnsupportedSellerRevenueShareTypes / timeDiffInSec)
      avgUnsupportedDealTypesPerSec = movingAverage(avgUnsupportedDealTypesPerSec, accUnsupportedDealTypes / timeDiffInSec)
      avgSendsPerSec = movingAverage(avgSendsPerSec, accSends / timeDiffInSec)

      accConsumes = 0
      accSelfDedups = 0
      accCouchbaseDedups = 0
      accFlattens = 0
      accMappingErrors = 0
      accCouchbaseErrors = 0
      accUnknownErrors = 0
      accCouchbaseDeserializationErrors = 0
      accDelayEvents = 0
      accUnParsedEvents = 0
      accUnknownEventTypes = 0
      accInvalidEvents = 0
      accPlacementNotFounds = 0
      accPublisherNotFounds = 0
      accExchangeRateNotFounds = 0
      accSecretKeyNotFounds = 0
      accDecryptClearPriceErrors = 0
      accUnsupportedPublisherRevenueShareTypes = 0
      accUnsupportedSellerRevenueShareTypes = 0
      accUnsupportedDealTypes = 0
      accSends = 0
    } else {
      accConsumes += consumeIncrementor
      accSelfDedups += selfDedupIncrementor
      accCouchbaseDedups += couchbaseDedupIncrementor
      accFlattens += flattenIncrementor
      accMappingErrors += mappingErrorIncrementor
      accCouchbaseErrors += couchbaseErrorIncrementor
      accUnknownErrors += unknownErrorIncrementor
      accCouchbaseDeserializationErrors += couchbaseDeserializationErrorIncrementor
      accDelayEvents += delayedEventIncrementor
      accUnParsedEvents += unParsedEventIncrementor
      accUnknownEventTypes += unknownEventTypeIncrementor
      accInvalidEvents += invalidEventIncrementor
      accPlacementNotFounds += placementNotFoundIncrementor
      accPublisherNotFounds += publisherNotFoundIncrementor
      accExchangeRateNotFounds += exchangeRateNotFoundIncrementor
      accSecretKeyNotFounds += secretKeyNotFoundIncrementor
      accDecryptClearPriceErrors += decryptClearPriceErrorIncrementor
      accUnsupportedPublisherRevenueShareTypes += unsupportedPublisherRevenueShareTypeIncrementor
      accUnsupportedSellerRevenueShareTypes += unsupportedSellerRevenueShareTypeIncrementor
      accUnsupportedDealTypes += unsupportedDealTypeIncrementor
      accSends += sendIncrementor
    }
  }

  private def movingAverage(acc: Double, adding: Double) = (acc * oldPercentage) + (adding * newPercentage)

  private[this] def infoStatsPresentation() = {
      JsObject(
        "akka" -> akkaMBeansStatsPresentation,
        "cache" -> allCacheStatsPresentation,
        "jvm" -> jvmMBeansStatsPresentation,
        "metrics" -> metricsStatsPresentation)
  }

  private[this] def allCacheStatsPresentation =
    JsObject(
      "DeviceLookup" -> cacheStatsPresentation(DeviceTypeMapping.cacheStats),
      "GeoLookup" -> cacheStatsPresentation(GeographyMapping.cacheStats)
    )

  private[this] def cacheStatsPresentation(stats: CacheStats) = {
    JsObject(
      "HitCount" -> JsNumber(stats.hitCount),
      "HitRate" -> JsNumber(stats.hitRate),
      "LoadCount" -> JsNumber(stats.loadCount),
      "LoadExceptionCount" -> JsNumber(stats.loadExceptionCount),
      "LoadExceptionRate" -> JsNumber(stats.loadExceptionRate),
      "LoadSuccessCount" -> JsNumber(stats.loadSuccessCount),
      "AverageLoadPenalty" -> JsNumber(stats.averageLoadPenalty),
      "EvictionCount" -> JsNumber(stats.evictionCount),
      "MissCount" -> JsNumber(stats.missCount),
      "MissRate" -> JsNumber(stats.missRate),
      "TotalLoadTime" -> JsNumber(stats.totalLoadTime)
    )
  }
}

