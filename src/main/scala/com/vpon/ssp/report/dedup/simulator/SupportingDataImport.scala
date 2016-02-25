package com.vpon.ssp.report.dedup.simulator

import scala.collection.mutable.ListBuffer

import spray.json._
import com.couchbase.client.java.CouchbaseCluster
import org.apache.commons.lang3.exception.ExceptionUtils
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.slf4j.LoggerFactory

import com.vpon.ssp.report.dedup.couchbase._
import com.vpon.ssp.report.dedup.generator.SupportingDataGenerator._
import com.vpon.ssp.report.dedup.model._
import PlacementObjectJsonProtocol._
import PublisherObjectJsonProtocol._
import ExchangeRateObjectJsonProtocol._


object SupportingDataImport extends App with GeneratorDrivenPropertyChecks {

  private val logger = LoggerFactory.getLogger(SupportingDataImport.getClass)

  case class ArgumentOptions(connectionString: Option[String] = None,
                             bucketName: Option[String] = None,
                             bucketPassword: Option[String] = None,
                             bucketKeyPrefix: Option[String] = None)

  val parser = new scopt.OptionParser[ArgumentOptions]("ssp-dedup-supporting-data") {
    head("ssp-dedup-supporting-data", "0.0.1")
    opt[String]('b', "connectionString") action { case (v, c) =>
      c.copy(connectionString = Some(v)) } text "connection string"
    opt[String]('n', "bucketName") action { case (v, c) =>
      c.copy(bucketName = Some(v)) } text "bucket name"
    opt[String]('p', "bucketPassword") action { case (v, c) =>
      c.copy(bucketPassword = Some(v)) } text "bucket password"
    opt[String]('k', "bucketKeyPrefix") action { case (v, c) =>
      c.copy(bucketKeyPrefix = Some(v)) } text "bucket key prefix"
    help("help") abbr "?" text "prints this usage text"
  }

  try {
    parser.parse(args, ArgumentOptions()) match {
      case Some(config) => appStart(config)
      case None => sys.exit(1)
    }
  } catch {
    case e: Throwable =>
      logger.error(ExceptionUtils.getStackTrace(e))
      sys.exit(2)
  }

  private def appStart(config: ArgumentOptions): Unit = {
    val connectionString = config.connectionString.getOrElse("http://localhost:8091")
    val bucketName = config.bucketName.getOrElse("ssp_web")
    val bucketPassword = config.bucketPassword.getOrElse("")
    val bucketKeyPrefix = config.bucketKeyPrefix.getOrElse("")

    val cluster = CouchbaseCluster.fromConnectionString(connectionString)
    val bucket = cluster.openBucket(bucketName, bucketPassword)
    CouchbaseUtil.removeAllDocs(bucket)

    val publisherPlacementObjectList: (Seq[(String/*platformId*/, String/*publisherId*/, PublisherObject)], Seq[(String/*platformId*/, String/*placementId*/, PlacementObject)]) = {
      val publisherObjectList = new ListBuffer[(String, String, PublisherObject)]()
      val placementObjectList = new ListBuffer[(String, String, PlacementObject)]()
      forAll(genPublisherObject) { // total 100
        (publisherObject: PublisherObject) => {
          val platformId = "1"
          val publisherId = (publisherObjectList.size + 1).toString
          publisherObjectList.append((platformId, publisherId, publisherObject))
          forAll(genPlacementObject(publisherId)) {
            (placementObject: PlacementObject) => {
              val placementId = (placementObjectList.size + 1).toString
              placementObjectList.append((platformId, placementId, placementObject))
            }
          }
        }
      }
      (publisherObjectList, placementObjectList)
    }

    val exchangeRateObjectList: Seq[(Int/*version*/, String/*currency*/, ExchangeRateObject)] = {
      val exchangeRateObjectList = new ListBuffer[(Int, String, ExchangeRateObject)]()
      val currencyMap: Map[String, (Double, Double)] = Map(
        "CNY" -> (5.25d, 6.25d),
        "TWD" -> (30.01d, 32.10d),
        "JPY" -> (101.03d, 108.72d),
        "HKD" -> (4.38d, 4.98d)
      )

      currencyMap.foreach {
        case (currency, (min, max)) => {
          var version = 1
          forAll(genExchangeRateObject(min, max)) {
            (exchangeRateObject: ExchangeRateObject) => {
              exchangeRateObjectList.append((version, currency, exchangeRateObject))
              version = version + 1
            }
          }
        }
      }
      exchangeRateObjectList
    }

    val publisherObjectList: Seq[(String, String, PublisherObject)] = publisherPlacementObjectList._1
    val placementObjectList: Seq[(String, String, PlacementObject)] = publisherPlacementObjectList._2

    // publisher
    publisherObjectList foreach(
      p => {
        val platformId = p._1
        val publisherId = p._2
        val publisherObject = p._3
        val docKey = PublisherObjectKey(publisherId, platformId, bucketKeyPrefix).docKey
        CouchbaseUtil.upsertData(docKey, publisherObject.toJson.compactPrint, bucket)
      }
      )

    // placement
    placementObjectList foreach(
      p => {
        val platformId = p._1
        val placementId = p._2
        val placementObject = p._3
        val docKey = PlacementObjectKey(placementId, platformId, bucketKeyPrefix).docKey
        CouchbaseUtil.upsertData(docKey, placementObject.toJson.compactPrint, bucket)
      }
      )

    // exchange rate
    exchangeRateObjectList foreach(
      e => {
        val (exchangeRateVersion, currency, exchangeRateObject) = e
        val docKey = ExchangeRateObjectKey(exchangeRateVersion, currency, bucketKeyPrefix).docKey
        CouchbaseUtil.upsertData(docKey, exchangeRateObject.toJson.compactPrint, bucket)
      }
      )

    bucket.close()
    cluster.disconnect()
    logger.debug("Insert supporting data done!")
  }

}
