package com.vpon.ssp.report.dedup

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.ExecutionContext.Implicits.global

import com.couchbase.client.core.CouchbaseException
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.error.DocumentDoesNotExistException
import com.google.common.cache.{CacheStats, Cache, CacheBuilder}
import org.apache.commons.lang3.exception.ExceptionUtils
import org.slf4j.LoggerFactory

import spray.json._

import com.vpon.ssp.report.dedup.couchbase._
import com.vpon.ssp.report.dedup.flatten.RetryWithThreadSleep.NeedRetryException
import com.vpon.ssp.report.dedup.flatten.exception.{CacheNotInitException, CouchbaseDeserializationException}
import com.vpon.ssp.report.dedup.model._

package object flatten {

  val WILDCARD = "*"
  val LANGUAGE_SPLITER = "-"
  val CURRENCY_USD = "USD"

  class InstantiateException(msg:String) extends RuntimeException(msg)

  // JJ: there is Scalacache, wrapper over some caches (including Guava's) that does some of these things. Wouldn't it help?
  trait BaseCache[V <: AnyRef] {

    private val logger = LoggerFactory.getLogger("BaseCache")

    private var cache: Cache[String, Option[V]] = _

    // kevinj: This is not thread-safe. make sure this is called in the beginning of app
    def init(spec: String) {
      cache = CacheBuilder.from(spec).recordStats().build()
    }

    def put(key: String, value: Option[V]): Unit = cache.put(key, value)

    // return null if key not found
    def get(key: String): Option[V] = {
      val value = cache.getIfPresent(key)
      Option(value) match {
        case None => None
        case Some(s) => s
      }
    }

    // trigger loader if key does not exist
    def getOrLoad(key: String)
                 (loader: String => Future[Option[V]])
                 (implicit ec: ExecutionContext): Future[Option[V]] = {
      Option(cache) match {
        case None => throw new CacheNotInitException("Cache Not Initialized")
        case Some(c) =>
      }
      val v = cache.getIfPresent(key)
      Option(v) match {
        case None => {
          logger.debug(s"loading data from db: $key")
          loader(key).map { value =>
            cache.put(key, value)
            value
          }
        }
        case Some(s) => {
          logger.debug(s"loaded data from cache: $key")
          Future.successful(s)
        }
      }
    }

    def invalidateAll(keys: Iterable[String]) {
      cache.invalidateAll(keys.asJava)
    }

    def invalidate(key: String) {
      cache.invalidate(key)
    }

    def flush(): Unit = {
      cache.invalidateAll()
    }

    def getStats: CacheStats = cache.stats
  }

  trait BaseCacheManager[T <: AnyRef] {

    val log = LoggerFactory.getLogger(this.getClass)

    val flattenBucket: RxCouchbaseBucket
    val flattenBucketKeyPrefix: String
    val couchbaseMaxRetries: Int
    val couchbaseRetryInterval: FiniteDuration

    val cache: BaseCache[T]

    protected def instantiate(v : JsonDocument): T

    protected def getData(key: String) = {
      cache.getOrLoad(key) { key =>
        retryGetData(key).map(v => {
          Some(instantiate(v))
        }).recover {
          case e: DocumentDoesNotExistException =>
            None
          case e: InstantiateException =>
            throw new CouchbaseDeserializationException(s"Failed to instantiate json document into object. DocumentID=${key}")
          case e: Exception =>
            throw new CouchbaseException(s"Failed to get json document from couchbase. DocumentID=${key}")
        }
      }
    }

    private def retryGetData(key: String): Future[JsonDocument] = {
      RetryWithThreadSleep(couchbaseMaxRetries, couchbaseRetryInterval.toMillis) {
        try {
          log.debug(s"fetching couchbase data with key $key from bucket ${flattenBucket.name}")
          flattenBucket.getJ(key)
        } catch {
          case e: DocumentDoesNotExistException =>
            log.warn(s"NOT found Couchbase Object for ${key}.")
            throw e
          case e: java.lang.NullPointerException =>
            val msg = s"java.lang.NullPointerException ===> Failed to get document id ${key} on CouchBase. Try again after ${couchbaseRetryInterval.toMillis}ms."
            log.warn(msg)
            throw new NeedRetryException(msg)
          case e: java.net.ConnectException =>
            val msg = s"java.net.ConnectException ===> Failed to remove document id ${key} on CouchBase. Try again after ${couchbaseRetryInterval.toMillis}ms."
            log.warn(msg)
            throw new NeedRetryException(msg)
          case e: RuntimeException if (e.getMessage.equals("java.util.concurrent.TimeoutException")) =>
            val msg = s"RuntimeException (java.util.concurrent.TimeoutException) when retryGetData. Try again after ${couchbaseRetryInterval.toMillis}ms."
            log.warn(msg)
            throw new NeedRetryException(msg)
          case e: Exception =>
            val msg = s"Failed to get object from couchbase. key:$key, error is: ${ExceptionUtils.getStackTrace(e)}"
            log.warn(msg)
            throw e
        }
      }
    }

  }

  object PlacementCache extends BaseCache[PlacementObject]
  object PublisherCache extends BaseCache[PublisherObject]
  object ExchangeRateCache extends BaseCache[ExchangeRateObject]
  object DspSspTaxRateCache extends BaseCache[DspSspTaxRateObject]
  object PublisherSspTaxRateCache extends BaseCache[PublisherSspTaxRateObject]

  class ExchangeRateCacheManager(val flattenBucket: RxCouchbaseBucket,
                                 val flattenBucketKeyPrefix: String,
                                 val couchbaseMaxRetries: Int,
                                 val couchbaseRetryInterval: FiniteDuration) extends BaseCacheManager[ExchangeRateObject] {
    val cache = ExchangeRateCache

    def instantiate(v: JsonDocument): ExchangeRateObject = {
      import ExchangeRateObjectJsonProtocol._
      val jsonString = v.content.toString
      log.debug(s"instantiating ExchangeRateObject from json $jsonString")
      try {
        val jsonAst = jsonString.parseJson
        jsonAst.convertTo[ExchangeRateObject]
      } catch {
        case e: Throwable => throw new InstantiateException(s"Failed to instantiate json into ExchangeRateObject. DocumentContent=$jsonString")
      }
    }

    def get(exchangeRateVersion: Int, currency: String): Future[Option[ExchangeRateObject]] = {
      val docKey = ExchangeRateObjectKey(exchangeRateVersion, currency, flattenBucketKeyPrefix).docKey
      getData(docKey)
    }
  }

  class PlacementCacheManager(val flattenBucket: RxCouchbaseBucket,
                              val flattenBucketKeyPrefix: String,
                              val couchbaseMaxRetries: Int,
                              val couchbaseRetryInterval: FiniteDuration) extends BaseCacheManager[PlacementObject] {
    val cache = PlacementCache

    def instantiate(v: JsonDocument): PlacementObject = {
      import PlacementObjectJsonProtocol._
      val jsonString = v.content.toString
      log.debug(s"instantiating PlacementObject from json $jsonString")
      try {
        val jsonAst = jsonString.parseJson
        jsonAst.convertTo[PlacementObject]
      } catch {
        case e: Throwable => throw new InstantiateException(s"Failed to instantiate json into PlacementObject. DocumentContent=$jsonString")
      }
    }

    def get(placementId: String, platformId: String): Future[Option[PlacementObject]] = {
      val docKey = PlacementObjectKey(placementId, platformId, flattenBucketKeyPrefix).docKey
      getData(docKey)
    }
  }

  class PublisherCacheManager(val flattenBucket: RxCouchbaseBucket,
                              val flattenBucketKeyPrefix: String,
                              val couchbaseMaxRetries: Int,
                              val couchbaseRetryInterval: FiniteDuration) extends BaseCacheManager[PublisherObject] {
    val cache = PublisherCache

    def instantiate(v: JsonDocument): PublisherObject = {
      import PublisherObjectJsonProtocol._
      val jsonString = v.content.toString
      log.debug(s"instantiating PublisherObject from json $jsonString")
      try {
        val jsonAst = jsonString.parseJson
        jsonAst.convertTo[PublisherObject]
      } catch {
        case e: Throwable => throw new InstantiateException(s"Failed to instantiate json into PublisherObject. DocumentContent=$jsonString")
      }
    }

    def get(publisherId: String, platformId: String): Future[Option[PublisherObject]] = {
      val docKey = PublisherObjectKey(publisherId, platformId, flattenBucketKeyPrefix).docKey
      getData(docKey)
    }
  }

  class DspSspTaxRateCacheManager(val flattenBucket: RxCouchbaseBucket,
                                  val flattenBucketKeyPrefix: String,
                                  val couchbaseMaxRetries: Int,
                                  val couchbaseRetryInterval: FiniteDuration) extends BaseCacheManager[DspSspTaxRateObject] {
    val cache = DspSspTaxRateCache

    def instantiate(v: JsonDocument): DspSspTaxRateObject = {
      import DspSspTaxRateObjectJsonProtocol._
      val jsonString = v.content.toString
      log.debug(s"instantiating DspSspTaxRateObject from json $jsonString")
      try {
        val jsonAst = jsonString.parseJson
        jsonAst.convertTo[DspSspTaxRateObject]
      } catch {
        case e: Throwable => throw new InstantiateException(s"Failed to instantiate json into DspSspTaxRateObject. DocumentContent=$jsonString")
      }
    }

    def get(dspSspTaxRateVersion: Int, dspId: String, platformId: String): Future[Option[DspSspTaxRateObject]] = {
      val docKey = DspSspTaxRateObjectKey(dspSspTaxRateVersion, dspId, platformId, flattenBucketKeyPrefix).docKey
      getData(docKey)
    }
  }

  class PublisherSspTaxRateCacheManager(val flattenBucket: RxCouchbaseBucket,
                                        val flattenBucketKeyPrefix: String,
                                        val couchbaseMaxRetries: Int,
                                        val couchbaseRetryInterval: FiniteDuration) extends BaseCacheManager[PublisherSspTaxRateObject] {
    val cache = PublisherSspTaxRateCache

    def instantiate(v: JsonDocument): PublisherSspTaxRateObject = {
      import PublisherSspTaxRateObjectJsonProtocol._
      val jsonString = v.content.toString
      log.debug(s"instantiating PublisherSspTaxRateObject from json $jsonString")
      try {
        val jsonAst = jsonString.parseJson
        jsonAst.convertTo[PublisherSspTaxRateObject]
      } catch {
        case e: Throwable => throw new InstantiateException(s"Failed to instantiate json into PublisherSspTaxRateObject. DocumentContent=$jsonString")
      }
    }

    def get(publisherSspTaxRateVersion: Int, publisherCountryCode: String, platformId: String): Future[Option[PublisherSspTaxRateObject]] = {
      val docKey = PublisherSspTaxRateObjectKey(publisherSspTaxRateVersion, publisherCountryCode, platformId, flattenBucketKeyPrefix).docKey
      getData(docKey)
    }
  }

  object RetryWithThreadSleep {
    class NeedRetryException(msg: String) extends Exception(msg)
    def apply[T](retries: Int, retryInterval: Long)(task: => T): T = {
      def retry(task: => T, leftRetries: Int): T = {
        try {
          task
        } catch {
          case _: NeedRetryException if leftRetries > 0 =>
            Thread.sleep(retryInterval)
            retry(task, leftRetries - 1)
          case e: Exception => throw e
        }
      }
      retry(task, retries)
    }
  }
}
