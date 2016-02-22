package com.vpon.ssp.report.dedup.flatten


import scala.concurrent.{Promise, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import org.apache.commons.lang3.exception.ExceptionUtils
import org.slf4j.LoggerFactory

import com.vpon.mapping.exception.MappingException
import com.vpon.ssp.report.common.model.{PlacementRecord, EventRecord, MediaRecord, TransactionRecord}
import com.vpon.ssp.report.common.util._
import com.vpon.ssp.report.dedup.flatten.exception.FlattenFailure
import com.vpon.ssp.report.common.model._

import scala.math.BigDecimal

import com.vpon.mapping._
import com.vpon.ssp.report.flatten.cryption.crypto.DES
import com.vpon.trade.Event.EVENTTYPE
import com.vpon.trade.Media.MEDIATYPE
import com.vpon.trade._

import com.vpon.ssp.report.dedup.flatten.config.FlattenConfig
import com.vpon.ssp.report.dedup.flatten.exception._

trait FlattenerTrait {
  def convert(data: Array[Byte], ignoreDelay: Boolean = false): Future[Either[FlattenFailure, EventRecord]]
}

object Flattener {

  // scalastyle:off
  private var flattener: Flattener = null
  // scalastyle:on

  def getInstance: Flattener = flattener

  def init(carrierMapping: CarrierMappingTrait = CarrierMapping,
           connectionTypeMapping: ConnectionTypeMappingTrait = ConnectionTypeMapping,
           languageMapping: LanguageMappingTrait = LanguageMapping,
           densityMapping: DensityMappingTrait = DensityMapping,
           geographyMapping: GeographyMappingTrait = GeographyMapping,
           deviceTypeMapping: DeviceTypeMappingTrait = DeviceTypeMapping,
           supplyTypeMapping: SupplyTypeMappingTrait = SupplyTypeMapping)(implicit flattenConfig: FlattenConfig): Unit = {

    geographyMapping.init(s"maximumSize=${flattenConfig.geographyMaxSize},initialCapacity=${flattenConfig.geographyInitialCapacity}," +
      s"concurrencyLevel=${flattenConfig.concurrencyLevel}")
    deviceTypeMapping.init(s"maximumSize=${flattenConfig.deviceMaxSize},initialCapacity=${flattenConfig.deviceInitialCapacity}," +
      s"concurrencyLevel=${flattenConfig.concurrencyLevel}")

    geographyMapping.warmUp()
    carrierMapping.warmUp()
    connectionTypeMapping.warmUp()
    densityMapping.warmUp()
    deviceTypeMapping.warmUp()
    languageMapping.warmUp()
    supplyTypeMapping.warmUp()

    flattener = new Flattener(flattenConfig, carrierMapping, connectionTypeMapping, languageMapping,
      densityMapping, geographyMapping, deviceTypeMapping, supplyTypeMapping)
  }

}

class Flattener private (flattenConfig: FlattenConfig,
                         theCarrierMapping: CarrierMappingTrait,
                         theConnectionTypeMapping: ConnectionTypeMappingTrait,
                         theLanguageMapping: LanguageMappingTrait,
                         theDensityMapping: DensityMappingTrait,
                         theGeographyMapping: GeographyMappingTrait,
                         theDeviceTypeMapping: DeviceTypeMappingTrait,
                         theSupplyTypeMapping: SupplyTypeMappingTrait) extends FlattenerTrait {

  private val logger = LoggerFactory.getLogger(Flattener.getClass)

  private[this] val bannerDelayPeriod = flattenConfig.bannerDelayPeriod
  private[this] val interstitialDelayPeriod = flattenConfig.interstitialDelayPeriod
  private[this] val secretKeyMap = flattenConfig.secretKeyMap
  private[this] val flattenBucket = flattenConfig.bucket
  private[this] val flattenBucketKeyPrefix = flattenConfig.bucketKeyPrefix

  private[this] val couchbaseMaxRetries = flattenConfig.couchbaseMaxRetries
  private[this] val couchbaseRetryInterval = flattenConfig.couchbaseRetryInterval

  private[this] val trackerPoolCapacity = 8192
  private[this] val trackerPool = new PoolWithBlockingQueue[TimeTracker](new TrackerPoolFactory, trackerPoolCapacity)

  val WILDCARD = "*"
  val LANGUAGE_SPLITER = "-"
  val CURRENCY_USD = "USD"

  @throws[RuntimeException]
  override def convert(data: Array[Byte], ignoreDelay: Boolean): Future[Either[FlattenFailure, EventRecord]] = {

    implicit val timeTracker = trackerPool.borrowObject()
    timeTracker.setTrackEnabled(logger.isDebugEnabled)

    val eventData: Option[Event] = try {
      timeTracker.track("before parse")
      Some(Event.parseFrom(data))
    } catch {
      case e: Exception => {
        None
      }
    }

    val p = Promise[Either[FlattenFailure, EventRecord]]

    eventData match {
      case None => p success Left(FlattenFailure(s"Unknown protobuf message: $data", FlattenFailureType.UnParsedEvent))
      case Some(event) => {
        val f = mappingFields(Future{event}, ignoreDelay)
        f onSuccess {
          case eventRecord => p success Right(eventRecord)
        }
        f onFailure {
          case e: MappingException => p success Left(FlattenFailure(getFailureMessage(e, event), FlattenFailureType.MappingError))
          case e: DelayedException => p success Left(FlattenFailure(getFailureMessage(e, event), FlattenFailureType.DelayedEvent))
          case e: InvalidEventException => p success Left(FlattenFailure(getFailureMessage(e, event), FlattenFailureType.InvalidEvent))
          case e: UnknownEventTypeException => p success Left(FlattenFailure(getFailureMessage(e, event), FlattenFailureType.UnknownEventType))
          case e: CouchbaseDeserializationException => p success Left(FlattenFailure(getFailureMessage(e, event), FlattenFailureType.CouchbaseDeserializationError))
          case e: PlacementObjectNotFoundException => p success Left(FlattenFailure(getFailureMessage(e, event), FlattenFailureType.PlacementNotFound))
          case e: PublisherObjectNotFoundException => p success Left(FlattenFailure(getFailureMessage(e, event), FlattenFailureType.PublisherNotFound))
          case e: ExchangeRateObjectNotFoundException => p success Left(FlattenFailure(getFailureMessage(e, event), FlattenFailureType.ExchangeRateNotFound))
          case e: PublisherSspTaxRateObjectNotFoundException => p success Left(FlattenFailure(getFailureMessage(e, event), FlattenFailureType.PublisherSspTaxRateNotFound))
          case e: DspSspTaxRateObjectNotFoundException => p success Left(FlattenFailure(getFailureMessage(e, event), FlattenFailureType.DspSspTaxRateNotFound))
          case e: CouchbaseException => p success Left(FlattenFailure(getFailureMessage(e, event), FlattenFailureType.CouchbaseError))
          case e: SecretKeyNotFoundException => p success Left(FlattenFailure(getFailureMessage(e, event), FlattenFailureType.SecretKeyNotFound))
          case e: DecryptClearPriceException => p success Left(FlattenFailure(getFailureMessage(e, event), FlattenFailureType.DecryptClearPriceError))
          case e: UnsupportedPublisherRevenueShareTypeException => p success Left(FlattenFailure(getFailureMessage(e, event), FlattenFailureType.UnsupportedPublisherRevenueShareType))
          case e: UnsupportedSellerRevenueShareTypeException => p success Left(FlattenFailure(getFailureMessage(e, event), FlattenFailureType.UnsupportedSellerRevenueShareType))
          case e: UnsupportedDealTypeException => p success Left(FlattenFailure(getFailureMessage(e, event), FlattenFailureType.UnsupportedDealType))
          case e: Exception => {
            logger.warn("Failed to mapping fields: " + ExceptionUtils.getStackTrace(e))
            p success Left(FlattenFailure(getFailureMessage(e, event), FlattenFailureType.UnknownError))
          }
        }
      }
    }
    p.future
  }

  private def getCommonFields(event: Event, ignoreDelay: Boolean) = {
    val eventType = event.`eventType`
    val eventKey = event.`eventKey`
    eventType match {
      case EVENTTYPE.TRADELOG =>
        if (event.`tradeLog`.isEmpty) {
          val err = s"The `trade_log` field should be defined in message if messageType is defined as TradeLog. eventKey: $eventKey. event: ${event.toJson()}"
          logger.warn(err)
          throw new InvalidEventException(err)
        }

        val tradeLogEvent = event.`tradeLog`.get

        val _bidId = tradeLogEvent.`bidId`
        val _bidTimestamp = tradeLogEvent.`bidTimestamp` // UTC with milliseconds

        val winner = tradeLogEvent.`winner`
        val (_dspId, _dspGroupId, _clearPrice, _dealType) = winner match {
          case Some(w) => {
            w.`dspGroupId` match {
              case None => {
                val err = s"The `dsp_group_id` field should be defined in BidWinner if messageType is defined as TradeLog and has a winner. eventKey: $eventKey. event: ${event.toJson()}"
                logger.warn(err)
                throw new InvalidEventException(err)
              }
              case Some(dgi) => {
                (Some(w.`dspId`), Some(dgi), BigDecimal(w.`clearPrice`), w.`dealType`)
              }
            }
          }
          case None => (None, None, BigDecimal(0.0), DEALTYPE.NO_PAYMENT)
        }

        val _impressionType = tradeLogEvent.`impressionType`
        val _placement = tradeLogEvent.`placement`
        val _rate = tradeLogEvent.`rate`
        val _media = tradeLogEvent.`media`
        val _device = tradeLogEvent.`device`
        val _user = tradeLogEvent.`user`
        val _site = tradeLogEvent.`site`
        val _app = tradeLogEvent.`app`
        val _sdk = tradeLogEvent.`sdk`

        val _bidPrice = if (tradeLogEvent.`winner`.isDefined) Some(BigDecimal(tradeLogEvent.`winner`.get.`bidPrice`)) else None

        (_bidId, _bidTimestamp, _dspId, _dspGroupId, _bidPrice, _clearPrice, _dealType, _impressionType, _placement, _rate, _media, _device, _user, _site, _app, _sdk)

      case EVENTTYPE.IMPRESSION =>
        if (event.`impression`.isEmpty) {
          val err = s"The `impression` field should be defined if messageType is defined as Impression. eventKey: $eventKey. event: ${event.toJson()}"
          logger.warn(err)
          throw new InvalidEventException(err)
        }

        val impressionEvent = event.`impression`.get
        val _bidId = impressionEvent.`bidId`
        val _bidTimestamp = impressionEvent.`bidTimestamp` // UTC with milliseconds
        val _dspId = impressionEvent.`dspId`
        val _dspGroupId: String = impressionEvent.`dspGroupId` match {
          case Some(dgi) => dgi
          case None => {
            val err = s"The `dsp_group_id` field should be defined if messageType is defined as Impression. eventKey: $eventKey. event: ${event.toJson()}"
            logger.warn(err)
            throw new InvalidEventException(err)
          }
        }
        val _clearPrice = decryptClearPrice(eventKey, _dspGroupId, impressionEvent.`clearPrice`)
        val _dealType = impressionEvent.`dealType`
        val _impressionType = impressionEvent.`impressionType`
        val _placement = impressionEvent.`placement`
        val _rate = impressionEvent.`rate`
        val _media = impressionEvent.`media`
        val _device = impressionEvent.`device`
        val _user = impressionEvent.`user`
        val _site = impressionEvent.`site`
        val _app = impressionEvent.`app`
        val _sdk = None

        val _bidPrice = None

        val _impressionTimestamp = impressionEvent.`impressionTimestamp` // UTC with milliseconds
        if (!ignoreDelay && isDelayed(_bidTimestamp, _impressionTimestamp, _media.`mediaType`)) {
          val err = s"The Impression event timestamp is ${_impressionTimestamp} which is exceed the delay threshold since Bid event happened timestamp ${_bidTimestamp}. " +
            s"eventKey: $eventKey. event: ${event.toJson()}"
          logger.warn(err)
          throw new DelayedException(err)
        }

        (_bidId, _bidTimestamp, Some(_dspId), Some(_dspGroupId), _bidPrice, _clearPrice, _dealType, _impressionType, _placement, _rate, _media, _device, _user, _site, _app, _sdk)

      case EVENTTYPE.CLICK =>
        if (event.`click`.isEmpty) {
          val err = s"The `click` field should be defined if messageType is defined as Click. eventKey: $eventKey. event: ${event.toJson()}"
          logger.warn(err)
          throw new InvalidEventException(err)
        }

        val clickEvent = event.`click`.get

        val _bidId = clickEvent.`bidId`
        val _bidTimestamp = clickEvent.`bidTimestamp` // UTC with milliseconds
        val _dspId = clickEvent.`dspId`
        val _dspGroupId: String = clickEvent.`dspGroupId` match {
          case Some(dgi) => dgi
          case None => {
            val err = s"The `dsp_group_id` field should be defined if messageType is defined as Click. eventKey: $eventKey. event: ${event.toJson()}"
            logger.warn(err)
            throw new InvalidEventException(err)
          }
        }
        val _clearPrice = decryptClearPrice(eventKey, _dspGroupId, clickEvent.`clearPrice`)
        val _dealType = clickEvent.`dealType`
        val _impressionType = clickEvent.`impressionType`
        val _placement = clickEvent.`placement`
        val _rate = clickEvent.`rate`
        val _media = clickEvent.`media`
        val _device = clickEvent.`device`
        val _user = clickEvent.`user`
        val _site = clickEvent.`site`
        val _app = clickEvent.`app`
        val _sdk = None

        val _bidPrice = None

        val _clickTimestamp = clickEvent.`clickTimestamp` // UTC with milliseconds
        if (!ignoreDelay && isDelayed(_bidTimestamp, _clickTimestamp, _media.`mediaType`)) {
          val err = s"The Click event timestamp is ${_clickTimestamp} which is exceed the delay threshold since Bid event happened timestamp ${_bidTimestamp}. " +
            s"eventKey: $eventKey. event: ${event.toJson()}"
          logger.warn(err)
          throw new DelayedException(err)
        }

        (_bidId, _bidTimestamp, Some(_dspId), Some(_dspGroupId), _bidPrice, _clearPrice, _dealType, _impressionType, _placement, _rate, _media, _device, _user, _site, _app, _sdk)

      case _ => {
        val err = s"Unknown EVENTTYPE: $eventType. eventKey: $eventKey. event: ${event.toJson()}"
        logger.warn(err)
        throw new UnknownEventTypeException(err)
      }
    }
  }

  private def getLanguageAndLocale(deviceLanguage: String) = {
    if (!deviceLanguage.isEmpty) {
      val languageItems = deviceLanguage.split(LANGUAGE_SPLITER)
      (languageItems(0), if (languageItems.length > 1) languageItems(1) else WILDCARD)
    } else {
      (WILDCARD, WILDCARD)
    }
  }

  private def mappingFields(eventF: Future[Event], ignoreDelay: Boolean)(implicit timeTracker: TimeTracker): Future[EventRecord] = {

    for {
      event <- eventF
      buyerTypeId = 0 // always set 0
      sellerTypeId = 0 // always set 0
      _ = timeTracker.track("enter mappingFields")
      eventType = event.`eventType`
      eventKey = event.`eventKey`
      (bidId, bidTimestamp, dspId, dspGroupId, bidPrice, clearPrice, dealType, impressionType, placement, rate, media, device, user, site, app, sdk) = getCommonFields(event, ignoreDelay)
      _ = validateDealType(dealType)
      placementId = placement.`placementId`
      platformId = placement.`platformId`
      exchangeRateVersion = rate.`exchangeRateVersion`
      publisherSspTaxRateVersion = rate.`publisherSspTaxRateVersion`
      dspSspTaxRateVersion = rate.`dspSspTaxRateVersion`
      bidFloorValue = BigDecimal(placement.`bidFloor`)
      bidFloorCur = placement.`bidFloorCur`
      mediaType = media.`mediaType`
      mediaFormat = media.`mediaFormat`
      iframeState = media.`iframeState`
      supplyType = media.`supplyType`
      adPosition = media.`adPosition`
      mediaWidth = media.`mediaWidth`
      mediaHeight = media.`mediaHeight`
      mediaSize = s"${mediaWidth}x${mediaHeight}"
      carrierMcc = device.`mobileCountryCode`
      carrierMnc = device.`mobileNetworkCode`
      vponCarrier = theCarrierMapping.findVponCarrierByMccAndMnc(carrierMcc.getOrElse(""), carrierMnc.getOrElse(""))
      _ = logger.debug(s"CarrierMapping.findVponCarrierByMccAndMnc\nInput: mcc-> ${carrierMcc.getOrElse("")}, mnc -> ${carrierMnc.getOrElse("")}\nOutput: vponCarrier -> $vponCarrier")
      vponCarrierId = vponCarrier.id
      _ = timeTracker.track("CarrierMapping.findVponCarrierByMccAndMnc")
      clientIp = device.`clientIp`
      connectionTypeCxnt = device.`connectionType`
      connectionTypeTelt = device.`telephonyType`
      vponConnectionType = theConnectionTypeMapping.findVponConnectionTypeByCxntAndTelt(connectionTypeCxnt.getOrElse(""), connectionTypeTelt.getOrElse(""))
      _ = logger.debug(s"ConnectionTypeMapping.findVponConnectionTypeByCxntAndTelt\nInput: cxnt-> ${connectionTypeCxnt.getOrElse("")}, telt -> ${connectionTypeTelt.getOrElse("")}" +
        s"\nOutput: vponConnectionType -> $vponConnectionType")
      vponConnectionTypeId = vponConnectionType.id
      _ = timeTracker.track("ConnectionTypeMapping.findVponConnectionTypeByCxntAndTelt")
      deviceLanguage = device.`language`
      (languageCode, locale) = getLanguageAndLocale(deviceLanguage)
      vponLanguage = theLanguageMapping.findVponLanguageByLanguageCodeAndLocale(languageCode, locale)
      _ = logger.debug(s"LanguageMapping.findVponLanguageByLanguageCodeAndLocale\nInput: languageCode-> $languageCode, locale -> $locale\nOutput: vponLanguage -> $vponLanguage")
      vponLanguageId = vponLanguage.id
      _ = timeTracker.track("LanguageMapping.findVponLanguageByLanguageCodeAndLocale")
      userAgent = device.`userAgent`
      screenWidth = device.`screenWidth`
      screenHeight = device.`screenHeight`
      vponDensity = theDensityMapping.findVponDensityByUserScreenDensity(device.`screenDensity`)
      _ = logger.debug(s"DensityMapping.findVponDensity\nInput: u_sd-> ${device.`screenDensity`}\nOutput: vponDensity -> $vponDensity")
      vponDensityId = vponDensity.id
      _ = timeTracker.track("DensityMapping.findVponDensity")
      screenAvailableWidth = device.`screenAvailableWidth`
      screenAvailableHeight = device.`screenAvailableHeight`
      vponGeoIds = theGeographyMapping.findVponGeoIdsByIP(clientIp)
      _ = logger.debug(s"==> GeographyMapping.findVponGeoIdsByIP\nInput: ip-> $clientIp\nOutput: vponGeoIds -> $vponGeoIds")
      _ = timeTracker.track("GeographyMapping.findVponGeoIdsByMaxmindGeo")
      vponGeo = theGeographyMapping.findVponGeoByVponGeoId(vponGeoIds.last)
      _ = logger.debug(s"==> GeographyMapping.findVponGeoByVponGeoId\nInput: vponGeoId-> ${vponGeoIds.last}\nOutput: vponGeo -> $vponGeo")
      _ = timeTracker.track("GeographyMapping.findVponGeoByVponGeoId")
      (geoCountry: String, geoRegion: String, geoCity: String) = vponGeo match {
        case Some(g) => (g.country, g.region, g.city)
        case None => ("", "", "")
      }
      vponDevice = theDeviceTypeMapping.findVponDeviceByUA(userAgent)
      _ = logger.debug(s"==> UserAgentMapping.findVponDevice\nInput: userAgent-> $userAgent\nOutput: vponDevice -> $vponDevice")
      _ = timeTracker.track("getDeviceObject")
      vponSupplyType = theSupplyTypeMapping.findVponSupplyTypeByPrimaryHardwareTypeAndSDKSupplyType(vponDevice.primaryHardwareType.getOrElse(""), supplyType.getNumber.toString)
      _ = logger.debug(s"==> SupplyTypeMapping.findVponSupplyType\nInput: uaPrimaryHardwareType-> ${vponDevice.primaryHardwareType.getOrElse("")}, " +
        s"sdk_st -> ${supplyType.getNumber.toString}\nOutput: vponSupplyType -> $vponSupplyType")
      _ = timeTracker.track("SupplyTypeMapping.findVponSupplyType")
      vponSupplyTypeId = vponSupplyType.id
      deviceMake = vponDevice.make.getOrElse("")
      deviceModel = vponDevice.model.getOrElse("")
      deviceOs = vponDevice.vponDeviceOsIds
      deviceType = vponDevice.vponDeviceTypeId
      userAge = user match {
        case Some(u) => u.`age` match {
          case Some(a) => a
          case None => -1
        }
        case None => -1
      }
      userGender = user match {
        case Some(u) => u.`gender` match {
          case Some(g) => g.getNumber
          case None => User.GENDERTYPE.UNKNOWN_GENDERTYPE.getNumber
        }
        case None => User.GENDERTYPE.UNKNOWN_GENDERTYPE.getNumber
      }
      (site_domain: String, site_protocol: Int) = site match {
        case Some(s) => (s.`siteDomain`, s.`siteProtocol`.getNumber)
        case None => ("", Site.PROTOCOLTYPE.UNKNOWN_PROTOCOLTYPE.getNumber)
      }
      app_id: String = app match {
        case Some(a) => a.`appId`
        case None => ""
      }
      deviceTime = device.`deviceTime`
      clientTimestamp: Option[Long] = deviceTime match {
        case Some(dt) => dt.`clientTimestamp`
        case None => None
      }
      clientTimezone: Option[Int] = deviceTime match {
        case Some(dt) => dt.`clientTimezone`
        case None => None
      }
    } yield {
      EventRecord(
        event_key = eventKey,
        bid_id = bidId,
        timestamp = bidTimestamp,
        event_type = eventType.getNumber,
        deviceSpecRecord = DeviceSpecRecord(
          device_type = deviceType,
          device_make = deviceMake,
          device_model = deviceModel,
          screen_width = screenWidth,
          screen_height = screenHeight,
          screen_density = vponDensityId
        ),
        deviceStatusRecord = DeviceStatusRecord(
          carrier_id = vponCarrierId,
          carrier_mcc = carrierMcc,
          carrier_mnc = carrierMnc,
          connection_type_id = vponConnectionTypeId,
          connection_type_cxnt = connectionTypeCxnt,
          connection_type_telt = connectionTypeTelt,
          device_os = deviceOs,
          user_agent = userAgent,
          client_timestamp = clientTimestamp,
          client_timezone = clientTimezone,
          client_ip = clientIp,
          language_id = vponLanguageId
        ),
        geographyRecord = GeographyRecord(
          geo_ids = vponGeoIds,
          city = geoCity,
          country = geoCountry,
          region = geoRegion
        ),
        placementRecord = PlacementRecord(
          media_format = mediaFormat,
          media_type = mediaType.getNumber,
          media_size = mediaSize,
          media_width = mediaWidth,
          media_height = mediaHeight,
          ad_position = adPosition match {
            case Some(adp) => adp.getNumber
            case None => Media.ADPOSITIONTYPE.UNKNOWN_ADPOSITIONTYPE.getNumber
          },
          placement_id = placementId,
          supply_type = vponSupplyTypeId
        ),
        mediaRecord = MediaRecord(
          iframe_state = iframeState match {
            case Some(s) => s.getNumber
            case None => Media.IFRAMESTATETYPE.UNKNOWN_IFRAMESTATETYPE.getNumber
          },
          screen_available_width = screenAvailableWidth,
          screen_available_height = screenAvailableHeight,
          site_protocol = site_protocol,
          site_domain = site_domain,
          app_id = app_id
        ),
        transactionRecord = TransactionRecord(
          buyer_type = buyerTypeId,
          buyer_id = dspId.getOrElse(""),
          buyer_group_id = dspGroupId.getOrElse(""),
          deal_type = dealType.getNumber,
          impression_type = impressionType.getNumber,
          seller_type = sellerTypeId,
          seller_id = platformId,
          bid_price = bidPrice,
          clear_price = clearPrice
        ),
        userRecord = UserRecord(
          user_age = userAge,
          user_gender = userGender
        ),
        sdkRecord = SdkRecord(
          sdk_version = sdk match {
            case Some(s) => Some(s.version)
            case None => None
          }
        )
      )
    }
  }

  private def getLanguageId(device: Device): Int = {
    val deviceLanguage = device.`language`
    val (languageCode, locale) = {
      if (!deviceLanguage.isEmpty) {
        val languageItems = deviceLanguage.split(LANGUAGE_SPLITER)
        (languageItems(0), if (languageItems.length > 1) languageItems(1) else WILDCARD)
      } else {
        (WILDCARD, WILDCARD)
      }
    }
    val language = theLanguageMapping.findVponLanguageByLanguageCodeAndLocale(languageCode, locale)
    logger.debug(s"LanguageMapping.findVponLanguageByLanguageCodeAndLocale\nInput: languageCode-> $languageCode, locale -> $locale\nOutput: language -> $language")
    language.id
  }

  private def isDelayed(t1: Long, t2: Long, media_type: MEDIATYPE.EnumVal): Boolean = {
    media_type match {
      case MEDIATYPE.BANNER => (t2 - t1) > bannerDelayPeriod.toMillis
      case MEDIATYPE.INTERSTITIAL => (t2 - t1) > interstitialDelayPeriod.toMillis
      case _ => {
        val err = s"isDelayed -> Unknown MEDIATYPE: ${media_type}."
        logger.warn(err)
        false
      }
    }
  }

  private def decryptClearPrice(eventKey: String, dspGroupId: String, encryptedClearPrice: String): BigDecimal = {
    logger.debug(s"decryptClearPrice: eventKey -> $eventKey, dspGroupId -> $dspGroupId, encryptedClearPrice -> $encryptedClearPrice")
    if (!secretKeyMap.contains(dspGroupId)) {
      val warning = s"Not found secret key for dsp group id: ${dspGroupId}"
      logger.warn(warning)
      throw new SecretKeyNotFoundException(warning)
    } else {
      logger.debug(s"Found secret key for dsp group id: ${dspGroupId}")
      if (encryptedClearPrice.isEmpty) {
        val warning = s"EncryptedClearPrice is Empty!"
        logger.warn(warning)
        throw new DecryptClearPriceException(warning)
      } else {
        val secretKey = secretKeyMap.get(dspGroupId).get.asInstanceOf[String]
        logger.debug(s"secretKey: $secretKey")
        try {
          val decryptString = new String(DES.base64DesDecrypt(encryptedClearPrice, secretKey))
          logger.debug(s"decryptString: $decryptString")
          BigDecimal(decryptString)
        } catch {
          case e: NumberFormatException => {
            val warning = s"Decrypted string ${new String(DES.base64DesDecrypt(encryptedClearPrice, secretKey))} is NOT a number"
            logger.warn(warning)
            throw new DecryptClearPriceException(warning)
          }
          case e: Throwable => {
            val warning = s"Failed to decrypt encrypted clear price! ${ExceptionUtils.getStackTrace(e)}"
            logger.warn(warning)
            throw new DecryptClearPriceException(warning)
          }
        }
      }
    }
  }

  private def getFailureMessage(e:Exception, event: Event): String = {
    s"${e.getMessage}. The event type is ${event.`eventType`}, event key is ${event.`eventKey`}, event content is ${event.toJson()}"
  }

  private def validateDealType(dealType: DEALTYPE.EnumVal): Unit = {
    dealType match {
      case DEALTYPE.CPA => throw new UnsupportedDealTypeException(s"Current system does NOT support deal type of ${DEALTYPE.CPA}")
      case _ =>
    }
  }

}