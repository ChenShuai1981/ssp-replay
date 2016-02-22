// scalastyle:off
package com.vpon.ssp.report.edge

import spray.json._

import org.slf4j.LoggerFactory

import JsonHelper._
import com.vpon.trade.{Event, DEALTYPE}

object AD_TYPE extends Enumeration {
  type TYPE = Value
  val UNKNOWN_AD_TYPE = Value(0, "UNKNOWN_AD_TYPE")
  val INTERSTITIAL = Value(1, "INTERSTITIAL")
  val BANNER = Value(2, "BANNER")
}

object DEAL_TYPE extends Enumeration {
  type TYPE = Value
  val UNKNOWN_DEAL_TYPE = Value(0, "UNKNOWN_DEAL_TYPE")
  val CPM = Value(1, "CPM")
  val CPC = Value(2, "CPC")
  val CPA = Value(3, "CPA")
}

object PAYMENTTYPE extends Enumeration {
  type TYPE = Value
  val NO_PAYMENT = Value(0, "NO_PAYMENT")
  val CPM = Value(1, "CPM")
  val CPC = Value(2, "CPC")
  val CPA = Value(3, "CPA")
  val OWNER_CPM = Value(4, "OWNER_CPM")
  val OWNER_REVENUE_SHARE = Value(5, "OWNER_REVENUE_SHARE")
}

object LOSER_REASON extends Enumeration {
  type TYPE = Value
  val UNKNOWN_REASON = Value(0, "UNKNOWN_REASON")
  val LOW_PRICE = Value(1, "LOW_PRICE")
  val TIMEOUT = Value(2, "TIMEOUT")
  val ADM_INVALID = Value(3, "ADM_INVALID")
  val RESPONSE_INVALID = Value(4, "RESPONSE_INVALID")
  val BID_ID_INVALID = Value(5, "BID_ID_INVALID")
  val BID_PRICE_INVALID = Value(6, "BID_PRICE_INVALID")
  val BID_WIN_NOTICE_INVALID = Value(7, "BID_WIN_NOTICE_INVALID")
  val BID_SEAT_INVALID = Value(8, "BID_SEAT_INVALID")
  val BID_ADM_TITLE_INVALID = Value(9, "BID_ADM_TITLE_INVALID")
  val BID_AUCTION_TYPE_INVALID = Value(10, "BID_AUCTION_TYPE_INVALID")
  val NOT_BIDDING = Value(22, "NOT_BIDDING")
}

object IMPRESSIONTYPE extends Enumeration {
  type TYPE = Value
  val UNKNOWN_IMPRESSION = Value(0, "UNKNOWN_IMPRESSION")
  val BLANK = Value(1, "BLANK")
  val DEFAULT = Value(2, "DEFAULT")
  val PSA = Value(3, "PSA")
  val KEPT = Value(4, "KEPT")
  val RESOLD = Value(5, "RESOLD")
  val RTB = Value(6, "RTB")
  val EXTERNAL_IMP = Value(7, "EXTERNAL_IMP")
  val EXTERNAL_CLK = Value(8, "EXTERNAL_CLK")
}

object MESSAGETYPE extends Enumeration {
  type TYPE = Value
  val UNKNOWN_MESSAGETYPE = Value(0, "UNKNOWN_MESSAGETYPE")
  val TRADELOG = Value(1, "TRADELOG")
  val IMPRESSION = Value(2, "IMPRESSION")
  val CLICK = Value(3, "CLICK")
}

object MEDIATYPE extends Enumeration {
  type TYPE = Value
  val UNKNOWN_MEDIATYPE = Value(0, "UNKNOWN_MEDIATYPE")
  val BANNER = Value(1, "BANNER")
  val INTERSTITIAL = Value(2, "INTERSTITIAL")
}

object SUPPLYTYPE extends Enumeration {
  type TYPE = Value
  val UNKNOWN_SUPPLYTYPE = Value(0, "UNKNOWN_SUPPLYTYPE")
  val WEB_PLACEMENTS = Value(1, "WEB_PLACEMENTS")
  val MOBILE_WEB_PLACEMENTS = Value(2, "MOBILE_WEB_PLACEMENTS")
  val APPS_PLACEMENTS = Value(3, "APPS_PLACEMENTS")
  val TV_PLACEMENTS = Value(4, "TV_PLACEMENTS")
}

object ADPOSITIONTYPE extends Enumeration {
  type TYPE = Value
  val UNKNOWN_ADPOSITIONTYPE = Value(0, "UNKNOWN_ADPOSITIONTYPE")
  val ABOVE_FOLD = Value(1, "ABOVE_FOLD")
  val DEPRECATED = Value(2, "DEPRECATED")
  val BELOW_FOLD = Value(3, "BELOW_FOLD")
  val HEADER = Value(4, "HEADER")
  val FOOTER = Value(5, "FOOTER")
  val SIDEBAR = Value(6, "SIDEBAR")
  val FULLSCREEN = Value(7, "FULLSCREEN")
}

object PROTOCOLTYPE extends Enumeration {
  type TYPE = Value
  val UNKNOWN_PROTOCOLTYPE = Value(0, "UNKNOWN_PROTOCOLTYPE")
  val HTTP = Value(1, "HTTP")
  val HTTPS = Value(2, "HTTPS")
}

object CONNECTIONTYPE extends Enumeration {
  type TYPE = Value
  val UNKNOWN_CONNECTIONTYPE = Value(0, "UNKNOWN_CONNECTIONTYPE")
  val ETHERNET = Value(1, "ETHERNET")
  val WIFI = Value(2, "WIFI")
  val CD_UNKNOWN = Value(3, "CD_UNKNOWN")
  val CD_2G = Value(4, "CD_2G")
  val CD_3G = Value(5, "CD_3G")
  val CD_4G = Value(6, "CD_4G")
}

object GENDERTYPE extends Enumeration {
  type TYPE = Value
  val UNKNOWN_GENDERTYPE = Value(0, "UNKNOWN_GENDERTYPE")
  val MALE = Value(1, "MALE")
  val FEMALE = Value(2, "FEMALE")
}

object Trade {
  private val logger = LoggerFactory.getLogger(Trade.getClass)
  
  case class BidResult(
                        dsp_id: String,
                        bid_lost_reason: LOSER_REASON.TYPE,
                        bid_price: Option[BigDecimal] = None,
                        currency: Option[String] = None,
                        deal_type: Option[DEAL_TYPE.TYPE] = None
                        )

  case class Rate(
                   exchange_rate_version: Int,
                   publisher_ssp_tax_rate_version: Int,
                   dsp_ssp_tax_rate_version: Int
                   ) {
    def toGetParam(): String = {
      "&exchange_rate_version=" + exchange_rate_version.toString +
        "&publisher_ssp_tax_rate_version=" + publisher_ssp_tax_rate_version.toString +
        "&dsp_ssp_tax_rate_version=" + dsp_ssp_tax_rate_version.toString
    }
  }

  case class TradePlacement(
                             placement_id: String,
                             platform_id: String,
                             bid_floor: BigDecimal,
                             bid_floor_cur: String
                             ) {
    def toGetParam(): String = {
      "&placement_id=" + placement_id + "&platform_id=" + platform_id + "&bid_floor=" + bid_floor + "&bid_floor_cur=" + bid_floor_cur
    }
  }

  case class Media(
                    media_type: MEDIATYPE.TYPE,
                    width: Int,
                    height: Int,
                    supply_type: Option[SUPPLYTYPE.TYPE] = None,
                    ad_position: Option[ADPOSITIONTYPE.TYPE] = None,
                    format: String,
                    iframe_state: Option[Int] = None
                    ) {
    def toGetParam(): String = {
      var params = "&media_type=" + media_type.id + "&media_width=" + width + "&media_height=" + height + "&media_format=" + format
      if (supply_type.isDefined) {
        params += "&supply_type=" + supply_type.get.id
      } else {
        params += "&supply_type=null"
      }

      if (ad_position.isDefined) {
        params += "&ad_position=" + ad_position.get.id
      } else {
        params += "&ad_position=null"
      }

      if (iframe_state.isDefined) {
        params += "&iframe_state=" + iframe_state.get
      } else {
        params += "&iframe_state=null"
      }

      params
    }
  }

  case class Site(
                   site_domain: Option[String] = None,
                   site_protocol: Option[PROTOCOLTYPE.TYPE] = None
                   ) {
    def toGetParam(): String = {
      var params = ""
      if (site_domain.isDefined) {
        params += "&site_domain=" + site_domain.get
      } else {
        params += "&site_domain="
      }

      if (site_protocol.isDefined) {
        params += "&site_protocol=" + site_protocol.get.id
      } else {
        params += "&site_protocol=0"
      }

      params
    }
  }

  case class App(app_id: Option[String] = None) {
    def toGetParam(): String = {
      var params = ""

      if (app_id.isDefined) {
        params += "&app_id=" + app_id.get
      } else {
        params += "&app_id="
      }

      params
    }
  }

  case class DeviceTime(
                         ts: Option[Long] = None,
                         tz: Option[Int] = None
                         )

  case class Device(
                     user_agent: String,
                     ip: String,
                     language: String,
                     screen_available_height: Option[Int] = None,
                     screen_available_width: Option[Int] = None,
                     screen_height: Option[Int] = None,
                     screen_width: Option[Int] = None,
                     screen_density: Option[Double] = None,
                     connection_type: Option[String] = None,
                     telephony_type: Option[String] = None,
                     mobile_country_code: Option[String] = None,
                     mobile_network_code: Option[String] = None,
                     device_time: Option[DeviceTime] = None
                     ) {
    def toGetParam(): String = {
      var params = "&client_ip=" + ip
      params += "&language=" + language
      if (screen_available_height.isDefined) {
        params += "&screen_available_height=" + screen_available_height.get
      } else {
        params += "&screen_available_height=null"
      }
      if (screen_available_width.isDefined) {
        params += "&screen_available_width=" + screen_available_width.get
      } else {
        params += "&screen_available_width=null"
      }
      if (screen_height.isDefined) {
        params += "&screen_height=" + screen_height.get
      } else {
        params += "&screen_height=null"
      }
      if (screen_width.isDefined) {
        params += "&screen_width=" + screen_width.get
      } else {
        params += "&screen_width=null"
      }
      if (screen_density.isDefined) {
        params += "&density=" + screen_density.get
      } else {
        params += "&density=null"
      }
      if (connection_type.isDefined) {
        params += "&cxnt=" + connection_type.get
      } else {
        params += "&cxnt="
      }
      if (telephony_type.isDefined) {
        params += "&telt=" + telephony_type.get
      } else {
        params += "&telt="
      }
      if (mobile_country_code.isDefined) {
        params += "&mcc=" + mobile_country_code.get
      } else {
        params += "&mcc="
      }
      if (mobile_network_code.isDefined) {
        params += "&mnc=" + mobile_network_code.get
      } else {
        params += "&mnc="
      }
      params
    }
  }

  case class SDK(version: Option[String] = None)

  case class User(
                   age: Option[Int] = None,
                   gender: Option[GENDERTYPE.TYPE] = None
                   ) {
    def toGetParam(): String = {
      var params = ""
      if (age.isDefined) {
        params += "&age=" + age.get
      } else {
        params += "&age=null"
      }
      if (gender.isDefined) {
        params += "&gender=" + gender.get.id
      } else {
        params += "&gender=null"
      }

      params
    }
  }

  case class BidWinner(
                        dsp_id: String,
                        bid_price: BigDecimal,
                        clear_price: BigDecimal,
                        deal_type: DEAL_TYPE.TYPE,
                        currency: String
                        )

  case class TradeLog(
                       bid_id: String,
                       bid_timestamp: Long,
                       impression_type: IMPRESSIONTYPE.TYPE,
                       placement: TradePlacement,
                       media: Media,
                       device: Device,
                       rate: Rate,
                       user: Option[User] = None,
                       site: Option[Site] = None,
                       app: Option[App] = None,
                       winner: Option[BidWinner] = None,
                       losers: Option[Seq[BidResult]] = None,
                       sdk: Option[SDK] = None
                       )

  case class TradeImpression(
                              impression_id: String,
                              impression_type: IMPRESSIONTYPE.TYPE,
                              bid_timestamp: Long,
                              bid_id: String,
                              clear_price: String,
                              dsp_id: String,
                              deal_type: DEAL_TYPE.TYPE,
                              placement: TradePlacement,
                              media: Media,
                              device: Device,
                              rate: Rate,
                              user: Option[User] = None,
                              site: Option[Site] = None,
                              app: Option[App] = None
                              ) {
    def toUserGetParam(): String = {
      "&age=null&gender=null"
    }

    def toSiteGetParam(): String = {
      "&site_domain=&site_protocol=0"
    }

    def toAppGetParam(): String = {
      "&app_id="
    }

    def toGetParam(): String = {
      var params = "impression_id=" + impression_id

      params += "&impression_type=" + impression_type.id
      params += "&bid_timestamp=" + bid_timestamp
      params += "&bid_id=" + bid_id
      params += "&clear_price=" + clear_price
      params += "&dsp_id=" + dsp_id
      params += "&deal_type=" + deal_type.id

      params += placement.toGetParam()
      params += media.toGetParam()
      params += device.toGetParam()
      params += rate.toGetParam()

      if (user.isDefined) {
        params += user.get.toGetParam()
      } else {
        params += toUserGetParam
      }

      if (site.isDefined) {
        params += site.get.toGetParam()
      } else {
        params += toSiteGetParam()
      }

      if (app.isDefined) {
        params += app.get.toGetParam()
      } else {
        params += toAppGetParam
      }

      params
    }
  }

  case class TradeClick(
                         click_id: String,
                         impression_type: IMPRESSIONTYPE.TYPE,
                         bid_timestamp: Long,
                         bid_id: String,
                         clear_price: String,
                         dsp_id: String,
                         deal_type: DEAL_TYPE.TYPE,
                         placement: TradePlacement,
                         media: Media,
                         device: Device,
                         rate: Rate,
                         user: Option[User] = None,
                         site: Option[Site] = None,
                         app: Option[App] = None
                         ) {
    def toUserGetParam(): String = {
      "&age=null&gender=null"
    }

    def toSiteGetParam(): String = {
      "&site_domain=&site_protocol=0"
    }

    def toAppGetParam(): String = {
      "&app_id="
    }

    def toGetParam(): String = {
      var params = "click_id=" + click_id
      params += "&impression_type=" + impression_type.id
      params += "&bid_timestamp=" + bid_timestamp
      params += "&bid_id=" + bid_id
      params += "&clear_price=" + clear_price
      params += "&dsp_id=" + dsp_id
      params += "&deal_type=" + deal_type.id

      params += placement.toGetParam()
      params += media.toGetParam()
      params += device.toGetParam()
      params += rate.toGetParam()

      if (user.isDefined) {
        params += user.get.toGetParam()
      } else {
        params += toUserGetParam
      }

      if (site.isDefined) {
        params += site.get.toGetParam()
      } else {
        params += toSiteGetParam
      }

      if (app.isDefined) {
        params += app.get.toGetParam()
      } else {
        params += toAppGetParam
      }

      params
    }
  }

  class InvalidEventException(message: String) extends RuntimeException(message)
  class UnknownImpressionTypeException(message: String) extends RuntimeException(message)
  class UnknownDealTypeException(message: String) extends RuntimeException(message)
  class UnknownEventTypeException(message: String) extends RuntimeException(message)
  class UnknownProtocolTypeException(message: String) extends RuntimeException(message)
  class UnknownGenderTypeException(message: String) extends RuntimeException(message)
  class UnknownSupplyTypeException(message: String) extends RuntimeException(message)
  class UnknownMediaTypeException(message: String) extends RuntimeException(message)
  class UnknownAdPositionTypeException(message: String) extends RuntimeException(message)
  class UnknownIFrameStateTypeException(message: String) extends RuntimeException(message)
  class UnknownBidLostReasonException(message: String) extends RuntimeException(message)

  case class EdgeConvertFailure(message: String, errorType: EdgeConvertFailureType.Value)

  object EdgeConvertFailureType extends Enumeration {
    type Failure = Value

    val InvalidEvent = Value("invalid_event")
    val UnknownImpressionType = Value("unknown_impression_type")
    val UnknownEventType = Value("unknown_event_type")
    val UnknownDealType = Value("unknown_deal_type")
    val UnknownProtocolType = Value("unknown_protocol_type")
    val UnknownBidLostReason = Value("unknown_bid_lost_reason")
    val UnknownGenderType = Value("unknown_gender_type")
    val UnknownSupplyType = Value("unknown_supply_type")
    val UnknownMediaType = Value("unknown_media_type")
    val UnknownAdPositionType = Value("unknown_ad_position_type")
    val UnknownIFrameStateType = Value("unknown_iframe_state_type")
    val UnknownError = Value("unknown_error")
  }

  case class EdgeEvent(event_type: MESSAGETYPE.TYPE,
                       event_key: String,
                       trade_log: Option[com.vpon.ssp.report.edge.Trade.TradeLog],
                       impression: Option[TradeImpression],
                       click: Option[TradeClick]) {
    def toEvent: Either[EdgeConvertFailure, com.vpon.trade.Event] = {
      try {
        val eventType = try {
          Event.EVENTTYPE.valueOf(event_type.id)
        } catch {
          case e: Throwable => throw new UnknownEventTypeException(s"Unknown event type id: ${event_type.id}")
        }
        logger.debug(s"eventType = $eventType")
        val eventKey = event_key
        logger.debug(s"eventKey = $eventKey")

        val event = eventType match {
          case Event.EVENTTYPE.TRADELOG => {
            trade_log match {
              case None => throw new InvalidEventException(s"Event type is TradeLog but can NOT find field 'trade_log'")
              case Some(tradeLog) => {
                new com.vpon.trade.Event(
                  eventType = Event.EVENTTYPE.TRADELOG,
                  eventKey = eventKey,
                  tradeLog = Some(toTradeLog(tradeLog)),
                  impression = None,
                  click = None
                )
              }
            }
          }
          case Event.EVENTTYPE.IMPRESSION => {
            impression match {
              case None => throw new InvalidEventException(s"Event type is Impression but can NOT find field 'impression'")
              case Some(imp) => {
                new com.vpon.trade.Event(
                  eventType = Event.EVENTTYPE.IMPRESSION,
                  eventKey = eventKey,
                  tradeLog = None,
                  impression = Some(toImpression(imp)),
                  click = None
                )
              }
            }
          }
          case Event.EVENTTYPE.CLICK => {
            click match {
              case None => throw new InvalidEventException(s"Event type is Click but can NOT find field 'click'")
              case Some(c) => {
                new com.vpon.trade.Event(
                  eventType = Event.EVENTTYPE.CLICK,
                  eventKey = eventKey,
                  tradeLog = None,
                  impression = None,
                  click = Some(toClick(c))
                )
              }
            }
          }
          case _ => throw new UnknownEventTypeException(s"Unknown event type: $eventType")
        }
        Right(event)
      } catch {
        case e: InvalidEventException => Left(EdgeConvertFailure(e.getMessage, EdgeConvertFailureType.InvalidEvent))
        case e: UnknownImpressionTypeException => Left(EdgeConvertFailure(e.getMessage, EdgeConvertFailureType.UnknownImpressionType))
        case e: UnknownDealTypeException => Left(EdgeConvertFailure(e.getMessage, EdgeConvertFailureType.UnknownDealType))
        case e: UnknownEventTypeException => Left(EdgeConvertFailure(e.getMessage, EdgeConvertFailureType.UnknownEventType))
        case e: UnknownProtocolTypeException => Left(EdgeConvertFailure(e.getMessage, EdgeConvertFailureType.UnknownProtocolType))
        case e: UnknownGenderTypeException => Left(EdgeConvertFailure(e.getMessage, EdgeConvertFailureType.UnknownGenderType))
        case e: UnknownSupplyTypeException => Left(EdgeConvertFailure(e.getMessage, EdgeConvertFailureType.UnknownSupplyType))
        case e: UnknownMediaTypeException => Left(EdgeConvertFailure(e.getMessage, EdgeConvertFailureType.UnknownMediaType))
        case e: UnknownAdPositionTypeException => Left(EdgeConvertFailure(e.getMessage, EdgeConvertFailureType.UnknownAdPositionType))
        case e: UnknownIFrameStateTypeException => Left(EdgeConvertFailure(e.getMessage, EdgeConvertFailureType.UnknownIFrameStateType))
        case e: UnknownBidLostReasonException => Left(EdgeConvertFailure(e.getMessage, EdgeConvertFailureType.UnknownBidLostReason))
        case e: Throwable => Left(EdgeConvertFailure(e.getMessage, EdgeConvertFailureType.UnknownError))
      }
    }

    private def toClick(edgeTradeClick: com.vpon.ssp.report.edge.Trade.TradeClick): com.vpon.trade.Click = {
      val clickId = edgeTradeClick.click_id
      val impressionType = try { com.vpon.trade.IMPRESSIONTYPE.valueOf(edgeTradeClick.impression_type.id) } catch {
        case e: Throwable => throw new UnknownImpressionTypeException(s"Unknown impression type id: ${edgeTradeClick.impression_type.id}")
      }
      val clickTimestamp = System.currentTimeMillis()
      val bidTimestamp = edgeTradeClick.bid_timestamp
      val bidId = edgeTradeClick.bid_id
      val clearPrice = edgeTradeClick.clear_price
      val dspId = edgeTradeClick.dsp_id
      val dealType = try { com.vpon.trade.DEALTYPE.valueOf(edgeTradeClick.deal_type.id) } catch {
        case e: Throwable => throw new UnknownDealTypeException(s"Unknown deal type id: ${edgeTradeClick.deal_type.id}")
      }
      val placement = toPlacement(edgeTradeClick.placement)
      val media = toMedia(edgeTradeClick.media)
      val device = toDevice(edgeTradeClick.device)
      val rate = toRate(edgeTradeClick.rate)
      val user = edgeTradeClick.user match {
        case None => None
        case Some(u) => Some(toUser(u))
      }
      val site = edgeTradeClick.site match {
        case None => None
        case Some(s) => Some(toSite(s))
      }
      val app = edgeTradeClick.app match {
        case None => None
        case Some(a) => Some(toApp(a))
      }

      com.vpon.trade.Click (
        clickId = clickId,
        impressionType = impressionType,
        clickTimestamp = clickTimestamp,
        bidTimestamp = bidTimestamp,
        bidId = bidId,
        clearPrice = clearPrice,
        dspId = dspId,
        dealType = dealType,
        placement = placement,
        media = media,
        device = device,
        rate = rate,
        user = user,
        site = site,
        app = app
      )
    }

    private def toImpression(edgeTradeImpression: com.vpon.ssp.report.edge.Trade.TradeImpression): com.vpon.trade.Impression = {
      val impressionId = edgeTradeImpression.impression_id
      val impressionType = try { com.vpon.trade.IMPRESSIONTYPE.valueOf(edgeTradeImpression.impression_type.id) } catch {
        case e: Throwable => throw new UnknownImpressionTypeException(s"Unknown impression type id: ${edgeTradeImpression.impression_type.id}")
      }
      val impressionTimestamp = System.currentTimeMillis()
      val bidTimestamp = edgeTradeImpression.bid_timestamp
      val bidId = edgeTradeImpression.bid_id
      val clearPrice = edgeTradeImpression.clear_price
      val dspId = edgeTradeImpression.dsp_id
      val dealType = try { com.vpon.trade.DEALTYPE.valueOf(edgeTradeImpression.deal_type.id) } catch {
        case e: Throwable => throw new UnknownDealTypeException(s"Unknown deal type id: ${edgeTradeImpression.deal_type.id}")
      }
      val placement = toPlacement(edgeTradeImpression.placement)
      val media = toMedia(edgeTradeImpression.media)
      val device = toDevice(edgeTradeImpression.device)
      val rate = toRate(edgeTradeImpression.rate)
      val user = edgeTradeImpression.user match {
        case None => None
        case Some(u) => Some(toUser(u))
      }
      val site = edgeTradeImpression.site match {
        case None => None
        case Some(s) => Some(toSite(s))
      }
      val app = edgeTradeImpression.app match {
        case None => None
        case Some(a) => Some(toApp(a))
      }

      com.vpon.trade.Impression (
        impressionId = impressionId,
        impressionType = impressionType,
        impressionTimestamp = impressionTimestamp,
        bidTimestamp = bidTimestamp,
        bidId = bidId,
        clearPrice = clearPrice,
        dspId = dspId,
        dealType = dealType,
        placement = placement,
        media = media,
        device = device,
        rate = rate,
        user = user,
        site = site,
        app = app
      )
    }

    private def toTradeLog(edgeTradeLog: com.vpon.ssp.report.edge.Trade.TradeLog): com.vpon.trade.TradeLog = {
      val bidId = edgeTradeLog.bid_id
      val bidTimestamp = edgeTradeLog.bid_timestamp
      val impressionType = try { com.vpon.trade.IMPRESSIONTYPE.valueOf(edgeTradeLog.impression_type.id) } catch {
        case e: Throwable => throw new UnknownImpressionTypeException(s"Unknown impression type id: ${edgeTradeLog.impression_type.id}")
      }
      val placement = toPlacement(edgeTradeLog.placement)
      val media = toMedia(edgeTradeLog.media)
      val device = toDevice(edgeTradeLog.device)
      val rate = toRate(edgeTradeLog.rate)
      val user = edgeTradeLog.user match {
        case None => None
        case Some(u) => Some(toUser(u))
      }
      val site = edgeTradeLog.site match {
        case None => None
        case Some(s) => Some(toSite(s))
      }
      val app = edgeTradeLog.app match {
        case None => None
        case Some(a) => Some(toApp(a))
      }
      val sdk = edgeTradeLog.sdk match {
        case None => None
        case Some(s) => Some(toSDK(s))
      }
      val winner = edgeTradeLog.winner match {
        case None => None
        case Some(w) => Some(toBidWinner(w))
      }
      val losers: scala.collection.immutable.Seq[com.vpon.trade.BidResult] = edgeTradeLog.losers match {
        case None => Vector.empty[com.vpon.trade.BidResult]
        case Some(xs) => {
          val losts = for (loser <- xs) yield toBidResult(loser)
          losts.toVector
        }
      }

      com.vpon.trade.TradeLog(
        bidId = bidId,
        bidTimestamp = bidTimestamp,
        impressionType = impressionType,
        placement = placement,
        media = media,
        device = device,
        rate = rate,
        user = user,
        site = site,
        app = app,
        winner = winner,
        losers = losers,
        sdk = sdk
      )
    }

    def toBidResult(edgeBidResult: com.vpon.ssp.report.edge.Trade.BidResult): com.vpon.trade.BidResult = {
      val dspId = edgeBidResult.dsp_id
      val bidLostReason = try { com.vpon.trade.BIDLOSTREASON.valueOf(edgeBidResult.bid_lost_reason.id) } catch {
        case e: Throwable => throw new UnknownBidLostReasonException(s"Unknown bid lost reason id: ${edgeBidResult.bid_lost_reason.id}")
      }
      val bidPrice = edgeBidResult.bid_price match {
        case None => None
        case Some(bp) => Some(bp.toDouble)
      }
      val currency = edgeBidResult.currency
      val dealType = edgeBidResult.deal_type match {
        case None => None
        case Some(dt) => Some(
          try { com.vpon.trade.DEALTYPE.valueOf(dt.id) } catch {
            case e: Throwable => throw new UnknownDealTypeException(s"Unknown deal type id: ${dt.id}")
          }
        )
      }

      com.vpon.trade.BidResult (
        dspId = dspId,
        bidLostReason = bidLostReason,
        bidPrice = bidPrice,
        currency = currency,
        dealType = dealType
      )
    }

    def toBidWinner(edgeBidWinner: com.vpon.ssp.report.edge.Trade.BidWinner): com.vpon.trade.BidWinner = {
      val dspId = edgeBidWinner.dsp_id
      val bidPrice = edgeBidWinner.bid_price.toDouble
      val clearPrice = edgeBidWinner.clear_price.toDouble
      val dealType = try { com.vpon.trade.DEALTYPE.valueOf(edgeBidWinner.deal_type.id) } catch {
        case e: Throwable => throw new UnknownDealTypeException(s"Unknown deal type id: ${edgeBidWinner.deal_type.id}")
      }
      val currency = edgeBidWinner.currency

      com.vpon.trade.BidWinner (
        dspId = dspId,
        bidPrice = bidPrice,
        clearPrice = clearPrice,
        dealType = dealType,
        currency = currency
      )
    }

    def toSDK(edgeSDK: com.vpon.ssp.report.edge.Trade.SDK): com.vpon.trade.SDK = {
      val version = edgeSDK.version.getOrElse("")
      com.vpon.trade.SDK (
        version = version
      )
    }

    def toApp(edgeApp: com.vpon.ssp.report.edge.Trade.App): com.vpon.trade.App = {
      val appId = edgeApp.app_id.getOrElse("")
      com.vpon.trade.App (
        appId = appId
      )
    }

    def toSite(edgeSite: com.vpon.ssp.report.edge.Trade.Site): com.vpon.trade.Site = {
      val siteDomain = edgeSite.site_domain.getOrElse("")
      val siteProtocol = edgeSite.site_protocol match {
        case None => com.vpon.trade.Site.PROTOCOLTYPE.UNKNOWN_PROTOCOLTYPE
        case Some(sp) => try { com.vpon.trade.Site.PROTOCOLTYPE.valueOf(sp.id) } catch {
          case e: Throwable => throw new UnknownProtocolTypeException(s"Unknown protocol type id: ${sp.id}")
        }
      }
      com.vpon.trade.Site (
        siteDomain = siteDomain,
        siteProtocol = siteProtocol
      )
    }

    def toUser(edgeUser: com.vpon.ssp.report.edge.Trade.User): com.vpon.trade.User = {
      val age = edgeUser.age
      val gender = edgeUser.gender match {
        case None => None
        case Some(g) => Some(
          try { com.vpon.trade.User.GENDERTYPE.valueOf(g.id) } catch {
            case e: Throwable => throw new UnknownGenderTypeException(s"Unknown gender type id: ${g.id}")
          }
        )
      }
      com.vpon.trade.User (
        age = age,
        gender = gender
      )
    }

    def toRate(edgeRate: com.vpon.ssp.report.edge.Trade.Rate): com.vpon.trade.Rate = {
      val exchangeRateVersion = edgeRate.exchange_rate_version
      val publisherSspTaxRateVersion = edgeRate.publisher_ssp_tax_rate_version
      val dspSspTaxRateVersion = edgeRate.dsp_ssp_tax_rate_version

      com.vpon.trade.Rate (
        exchangeRateVersion = exchangeRateVersion,
        publisherSspTaxRateVersion = publisherSspTaxRateVersion,
        dspSspTaxRateVersion = dspSspTaxRateVersion
      )
    }

    def toDevice(edgeDevice: com.vpon.ssp.report.edge.Trade.Device): com.vpon.trade.Device = {
      val userAgent = edgeDevice.user_agent
      val clientIp = edgeDevice.ip
      val language = edgeDevice.language
      val screenAvailableHeight = edgeDevice.screen_available_height
      val screenAvailableWidth = edgeDevice.screen_available_width
      val screenHeight = edgeDevice.screen_height
      val screenWidth = edgeDevice.screen_width
      val screenDensity = edgeDevice.screen_density
      val connectionType = edgeDevice.connection_type
      val telephonyType = edgeDevice.telephony_type
      val mobileCountryCode = edgeDevice.mobile_country_code
      val mobileNetworkCode = edgeDevice.mobile_network_code
      val deviceTime = edgeDevice.device_time match {
        case None => None
        case Some(time) => Some(com.vpon.trade.Device.DeviceTime(
          clientTimestamp = time.ts,
          clientTimezone = time.tz
         ))
      }

      com.vpon.trade.Device (
        userAgent = userAgent,
        clientIp = clientIp,
        language = language,
        screenAvailableHeight = screenAvailableHeight,
        screenAvailableWidth = screenAvailableWidth,
        screenHeight = screenHeight,
        screenWidth = screenWidth,
        screenDensity = screenDensity,
        connectionType = connectionType,
        telephonyType = telephonyType,
        mobileCountryCode = mobileCountryCode,
        mobileNetworkCode = mobileNetworkCode,
        deviceTime = deviceTime
      )
    }

    def toMedia(edgeMedia: com.vpon.ssp.report.edge.Trade.Media): com.vpon.trade.Media = {
      val mediaType = try { com.vpon.trade.Media.MEDIATYPE.valueOf(edgeMedia.media_type.id) } catch {
        case e: Throwable => throw new UnknownMediaTypeException(s"Unknown media type id: ${edgeMedia.media_type.id}")
      }
      val mediaHeight = edgeMedia.height
      val mediaWidth = edgeMedia.width
      val supplyType = edgeMedia.supply_type match {
        case None => com.vpon.trade.Media.SUPPLYTYPE.UNKNOWN_SUPPLYTYPE
        case Some(st) => try { com.vpon.trade.Media.SUPPLYTYPE.valueOf(st.id) } catch {
          case e: Throwable => throw new UnknownSupplyTypeException(s"Unknown supply type id: ${st.id}")
        }
      }
      val adPosition = edgeMedia.ad_position match {
        case None => com.vpon.trade.Media.ADPOSITIONTYPE.UNKNOWN_ADPOSITIONTYPE
        case Some(ap) => try { com.vpon.trade.Media.ADPOSITIONTYPE.valueOf(ap.id) } catch {
          case e: Throwable => throw new UnknownAdPositionTypeException(s"Unknown ad position type id: ${ap.id}")
        }
      }
      val iframeState = edgeMedia.iframe_state match {
        case None => com.vpon.trade.Media.IFRAMESTATETYPE.UNKNOWN_IFRAMESTATETYPE
        case Some(ifs) => try { com.vpon.trade.Media.IFRAMESTATETYPE.valueOf(ifs) } catch {
          case e: Throwable => throw new UnknownIFrameStateTypeException(s"Unknown ad iframe state type id: ${ifs}")
        }
      }

      com.vpon.trade.Media (
        mediaType = mediaType,
        mediaHeight = mediaHeight,
        mediaWidth = mediaWidth,
        supplyType = supplyType,
        adPosition = Some(adPosition),
        iframeState = Some(iframeState)
      )
    }

    def toPlacement(edgePlacement: TradePlacement): com.vpon.trade.Placement = {
      val placementId = edgePlacement.placement_id
      val platformId = edgePlacement.platform_id
      val bidFloor = edgePlacement.bid_floor.toDouble
      val bidFloorCur = edgePlacement.bid_floor_cur

      com.vpon.trade.Placement (
        placementId = placementId,
        platformId = platformId,
        bidFloor = bidFloor,
        bidFloorCur = bidFloorCur
      )
    }
  }

  object TradeLogJsonProtocol extends DefaultJsonProtocol {
    implicit object BidResultFormat extends RootJsonFormat[BidResult] {
      def write(v: BidResult) = {
        val params = collection.mutable.ArrayBuffer.empty[JsField]

        params += "dsp_id" -> JsString(v.dsp_id)
        params += "bid_lost_reason" -> JsNumber(v.bid_lost_reason.id)
        if (v.bid_price.isDefined) {
          params += "bid_price" -> JsNumber(v.bid_price.get)
        }
        if (v.currency.isDefined) {
          params += "currency" -> JsString(v.currency.get)
        }
        if (v.deal_type.isDefined) {
          params += "deal_type" -> JsNumber(v.deal_type.get.id)
        }

        JsObject(params.toMap)
      }

      def read(value: JsValue) = {
        val fields = value.asJsObject.fields
        val dsp_id = fields.checkExist("dsp_id").strValue
        val bid_lost_reason = fields.checkExist("bid_lost_reason").intValue
        val bid_price = fields.get("bid_price").optBigDecimal
        val currency = fields.get("currency").optStr
        val deal_type: Option[DEAL_TYPE.TYPE] = fields.get("deal_type").optInt match {
          case Some(d) => {
            d match {
              case x if x >= DEAL_TYPE.UNKNOWN_DEAL_TYPE.id && x <= DEAL_TYPE.CPA.id => { Some(DEAL_TYPE.apply(x)) }
              case _ => { Some(DEAL_TYPE.UNKNOWN_DEAL_TYPE) }
            }
          }
          case _ => { None }
        }

        BidResult(
          dsp_id = dsp_id,
          bid_lost_reason = LOSER_REASON.apply(bid_lost_reason - 1),
          bid_price = bid_price,
          currency = currency,
          deal_type = deal_type
        )
      }
    }

    implicit object RateFormat extends RootJsonFormat[Rate] {
      def write(v: Rate) = {
        val params = collection.mutable.ArrayBuffer.empty[JsField]

        params += "exchange_rate_version" -> JsNumber(v.exchange_rate_version)
        params += "publisher_ssp_tax_rate_version" -> JsNumber(v.publisher_ssp_tax_rate_version)
        params += "dsp_ssp_tax_rate_version" -> JsNumber(v.dsp_ssp_tax_rate_version)

        JsObject(params.toMap)
      }

      def read(value: JsValue) = {
        val fields = value.asJsObject.fields
        val exchange_rate_version = fields.checkExist("exchange_rate_version").intValue
        val publisher_ssp_tax_rate_version = fields.checkExist("publisher_ssp_tax_rate_version").intValue
        val dsp_ssp_tax_rate_version = fields.checkExist("dsp_ssp_tax_rate_version").intValue

        Rate(
          exchange_rate_version = exchange_rate_version,
          publisher_ssp_tax_rate_version = publisher_ssp_tax_rate_version,
          dsp_ssp_tax_rate_version = dsp_ssp_tax_rate_version
        )
      }
    }

    implicit object SiteFormat extends RootJsonFormat[Site] {
      def write(v: Site) = {
        val params = collection.mutable.ArrayBuffer.empty[JsField]

        if (v.site_domain.isDefined) {
          params += "site_domain" -> JsString(v.site_domain.get)
        }

        if (v.site_protocol.isDefined) {
          params += "site_protocol" -> JsNumber(v.site_protocol.get.id)
        }

        JsObject(params.toMap)
      }

      def read(value: JsValue) = {
        val fields = value.asJsObject.fields
        val site_domain = fields.get("site_domain").optStr
        val site_protocol: Option[PROTOCOLTYPE.TYPE] = fields.get("site_protocol").optInt match {
          case Some(p) => {
            p match {
              case x if x >= PROTOCOLTYPE.UNKNOWN_PROTOCOLTYPE.id && x <= PROTOCOLTYPE.HTTPS.id => {
                Some(PROTOCOLTYPE.apply(x))
              }
              case _ => { Some(PROTOCOLTYPE.UNKNOWN_PROTOCOLTYPE) }
            }
          }
          case _ => { None }
        }

        Site(
          site_domain = site_domain,
          site_protocol = site_protocol
        )
      }
    }

    implicit object TradePlacementFormat extends RootJsonFormat[TradePlacement] {
      def write(v: TradePlacement) = {
        val params = collection.mutable.ArrayBuffer.empty[JsField]

        params += "placement_id" -> JsString(v.placement_id)
        params += "platform_id" -> JsString(v.platform_id)
        params += "bid_floor" -> JsNumber(v.bid_floor)
        params += "bid_floor_cur" -> JsString(v.bid_floor_cur)

        JsObject(params.toMap)
      }

      def read(value: JsValue) = {
        val fields = value.asJsObject.fields
        val placement_id = fields.checkExist("placement_id").strValue
        val platform_id = fields.checkExist("platform_id").strValue
        val bid_floor = fields.checkExist("bid_floor").optBigDecimal
        val bid_floor_cur = fields.checkExist("bid_floor_cur").strValue

        TradePlacement(
          placement_id = placement_id,
          platform_id = platform_id,
          bid_floor = bid_floor.get,
          bid_floor_cur = bid_floor_cur
        )
      }
    }

    implicit object MediaFormat extends RootJsonFormat[Media] {
      def write(v: Media) = {
        val params = collection.mutable.ArrayBuffer.empty[JsField]

        params += "media_type" -> JsNumber(v.media_type.id)
        params += "media_width" -> JsNumber(v.width)
        params += "media_height" -> JsNumber(v.height)
        params += "media_format" -> JsString(v.format)
        if (v.supply_type.isDefined) {
          params += "supply_type" -> JsNumber(v.supply_type.get.id)
        }
        if (v.ad_position.isDefined) {
          params += "ad_position" -> JsNumber(v.ad_position.get.id)
        }
        if (v.iframe_state.isDefined) {
          params += "iframe_state" -> JsNumber(v.iframe_state.get)
        }

        JsObject(params.toMap)
      }

      def read(value: JsValue) = {
        val fields = value.asJsObject.fields
        val media_type = fields.checkExist("media_type").intValue
        val width = fields.checkExist("media_width").intValue
        val height = fields.checkExist("media_height").intValue
        val supply_type: Option[SUPPLYTYPE.TYPE] = fields.get("supply_type").optInt match {
          case Some(s) => {
            s match {
              case x if x >= SUPPLYTYPE.UNKNOWN_SUPPLYTYPE.id && x <= SUPPLYTYPE.TV_PLACEMENTS.id => {
                Some(SUPPLYTYPE.apply(x))
              }
              case _ => {
                Some(SUPPLYTYPE.UNKNOWN_SUPPLYTYPE)
              }
            }
          }
          case _ => { None }
        }
        val ad_position: Option[ADPOSITIONTYPE.TYPE] = fields.get("ad_position").optInt match {
          case Some(a) => {
            a match {
              case x if x >= ADPOSITIONTYPE.UNKNOWN_ADPOSITIONTYPE.id && x <= ADPOSITIONTYPE.FULLSCREEN.id => {
                Some(ADPOSITIONTYPE.apply(x))
              }
              case _ => {
                Some(ADPOSITIONTYPE.UNKNOWN_ADPOSITIONTYPE)
              }
            }
          }
          case _ => { None }
        }
        val format = fields.checkExist("media_format").strValue
        val iframe_state = fields.get("iframe_state").optInt match {
          case Some(i) => {
            i match {
              case x if x >= 0 && x <= 3 => { Some(x) }
              case _ => { Some(0) }
            }
          }
          case _ => { Some(0) }
        }

        Media(
          media_type = MEDIATYPE.apply(media_type),
          width = width,
          height = height,
          supply_type = supply_type,
          ad_position = ad_position,
          format = format,
          iframe_state = iframe_state
        )
      }
    }

    implicit object AppFormat extends RootJsonFormat[App] {
      def write(v: App) = {
        val params = collection.mutable.ArrayBuffer.empty[JsField]

        if (v.app_id.isDefined) {
          params += "app_id" -> JsString(v.app_id.get)
        }

        JsObject(params.toMap)
      }

      def read(value: JsValue) = {
        val fields = value.asJsObject.fields
        val app_id = fields.get("app_id").optStr
        App(
          app_id = app_id
        )
      }
    }

    implicit object DeviceFormat extends RootJsonFormat[Device] {
      def write(v: Device) = {
        val params = collection.mutable.ArrayBuffer.empty[JsField]

        params += "user_agent" -> JsString(v.user_agent)
        params += "client_ip" -> JsString(v.ip)
        params += "language" -> JsString(v.language)
        if (v.screen_available_height.isDefined) {
          params += "screen_available_height" -> JsNumber(v.screen_available_height.get)
        }
        if (v.screen_available_width.isDefined) {
          params += "screen_available_width" -> JsNumber(v.screen_available_width.get)
        }
        if (v.screen_height.isDefined) {
          params += "screen_height" -> JsNumber(v.screen_height.get)
        }
        if (v.screen_width.isDefined) {
          params += "screen_width" -> JsNumber(v.screen_width.get)
        }
        if (v.screen_density.isDefined) {
          params += "screen_density" -> JsNumber(v.screen_density.get)
        }
        if (v.connection_type.isDefined) {
          params += "connection_type" -> JsString(v.connection_type.get)
        }
        if (v.telephony_type.isDefined) {
          params += "telephony_type" -> JsString(v.telephony_type.get)
        }
        if (v.mobile_country_code.isDefined) {
          params += "mobile_country_code" -> JsString(v.mobile_country_code.get)
        }
        if (v.mobile_network_code.isDefined) {
          params += "mobile_network_code" -> JsString(v.mobile_network_code.get)
        }

        JsObject(params.toMap)
      }

      def read(value: JsValue) = {
        val fields = value.asJsObject.fields
        val user_agent = fields.checkExist("user_agent").strValue
        logger.debug(s"user_agent = ${user_agent}")
        val ip = fields.checkExist("client_ip").strValue
        logger.debug(s"ip = ${ip}")
        val language = fields.checkExist("language").strValue
        logger.debug(s"language = ${language}")
        val carrier = fields.get("carrier").optStr
        logger.debug(s"carrier = ${carrier}")
        val screen_available_height = fields.get("screen_available_height").optInt
        logger.debug(s"screen_available_height = ${screen_available_height}")
        val screen_available_width = fields.get("screen_available_width").optInt
        logger.debug(s"screen_available_width = ${screen_available_width}")
        val screen_height = fields.get("screen_height").optInt
        logger.debug(s"screen_height = ${screen_height}")
        val screen_width = fields.get("screen_width").optInt
        logger.debug(s"screen_width = ${screen_width}")
        val screen_density = fields.get("screen_density").optDouble
        logger.debug(s"screen_density = ${screen_density}")
        val connection_type = fields.get("connection_type").optStr
        logger.debug(s"connection_type = ${connection_type}")
        val telephony_type = fields.get("telephony_type").optStr
        logger.debug(s"telephony_type = ${telephony_type}")
        val mobile_country_code = fields.get("mobile_country_code").optStr
        logger.debug(s"mobile_country_code = ${mobile_country_code}")
        val mobile_network_code = fields.get("mobile_network_code").optStr
        logger.debug(s"mobile_network_code = ${mobile_network_code}")

        Device(
          user_agent = user_agent,
          ip = ip,
          language = language,
          screen_available_height = screen_available_height,
          screen_available_width = screen_available_width,
          screen_height = screen_height,
          screen_width = screen_width,
          screen_density = screen_density,
          connection_type = connection_type,
          telephony_type = telephony_type,
          mobile_country_code = mobile_country_code,
          mobile_network_code = mobile_network_code
        )
      }
    }

    implicit object SDKFormat extends RootJsonFormat[SDK] {
      def write(v: SDK) = {
        val params = collection.mutable.ArrayBuffer.empty[JsField]

        if (v.version.isDefined) {
          params += "version" -> JsString(v.version.get)
        }

        JsObject(params.toMap)
      }

      def read(value: JsValue) = {
        val fields = value.asJsObject.fields
        val version = fields.get("version").optStr

        SDK(
          version = version
        )
      }
    }

    implicit object UserFormat extends RootJsonFormat[User] {
      def write(v: User) = {
        val params = collection.mutable.ArrayBuffer.empty[JsField]

        if (v.age.isDefined) {
          params += "age" -> JsNumber(v.age.get)
        }

        if (v.gender.isDefined) {
          params += "gender" -> JsNumber(v.gender.get.id)
        }

        JsObject(params.toMap)
      }

      def read(value: JsValue) = {
        val fields = value.asJsObject.fields
        val gender = fields.get("gender").optInt
        val age = fields.get("age").optInt

        val gender_type = gender match {
          case Some(g) => {
            Some(GENDERTYPE.apply(g))
          }
          case _ => { None }
        }

        User(
          gender = gender_type,
          age = age
        )
      }
    }

    implicit object BidWinnerFormat extends RootJsonFormat[BidWinner] {
      def write(v: BidWinner) = {
        val params = collection.mutable.ArrayBuffer.empty[JsField]

        params += "dsp_id" -> JsString(v.dsp_id)
        params += "bid_price" -> JsNumber(v.bid_price)
        params += "clear_price" -> JsNumber(v.clear_price)
        params += "deal_type" -> JsNumber(v.deal_type.id)
        params += "currency" -> JsString(v.currency)

        JsObject(params.toMap)
      }

      def read(value: JsValue) = {
        val fields = value.asJsObject.fields
        val dsp_id = fields.checkExist("dsp_id").strValue
        val bid_price = fields.checkExist("bid_price").decimalValue
        val clear_price = fields.checkExist("clear_price").decimalValue
        val deal_type = fields.checkExist("deal_type").intValue
        val currency = fields.checkExist("currency").strValue

        BidWinner(
          dsp_id = dsp_id,
          bid_price = bid_price,
          clear_price = clear_price,
          deal_type = DEAL_TYPE.apply(deal_type - 1),
          currency = currency
        )
      }
    }

    implicit object TradeLogFormat extends RootJsonFormat[TradeLog] {
      def write(v: TradeLog) = {
        val params = collection.mutable.ArrayBuffer.empty[JsField]

        params += "bid_id" -> JsString(v.bid_id)
        params += "bid_timestamp" -> JsNumber(v.bid_timestamp)
        params += "impression_type" -> JsNumber(v.impression_type.id)
        params += "placement" -> TradePlacementFormat.write(v.placement)
        params += "media" -> MediaFormat.write(v.media)
        params += "device" -> DeviceFormat.write(v.device)
        params += "rate" -> RateFormat.write(v.rate)

        if (v.user.isDefined) {
          params += "user" -> UserFormat.write(v.user.get)
        }
        if (v.site.isDefined) {
          params += "site" -> SiteFormat.write(v.site.get)
        }
        if (v.app.isDefined) {
          params += "app" -> AppFormat.write(v.app.get)
        }

        if (v.winner.isDefined) {
          params += "winner" -> BidWinnerFormat.write(v.winner.get)
        }

        if (v.losers.isDefined) {
          params += "losers" -> seqFormat[BidResult].write(v.losers.get)
        }

        JsObject(params.toMap)
      }

      def read(value: JsValue) = {
        val fields = value.asJsObject.fields
        val bid_id = fields.checkExist("bid_id").strValue
        val bid_timestamp = fields.checkExist("bid_timestamp").longValue
        val impression = fields.checkExist("impression_type").intValue
        val placement = TradePlacementFormat.read(fields.checkExist("placement").get)
        val media = MediaFormat.read(fields.checkExist("media").get)
        val device = DeviceFormat.read(fields.checkExist("device").get)
        val user = fields.get("user").optFormat[User]
        val site = fields.get("site").optFormat[Site]
        val app = fields.get("app").optFormat[App]
        val winner = fields.checkExist("winner").optFormat[BidWinner]
        val rate = RateFormat.read(fields.checkExist("rate").get)
        val losers = fields.checkExist("losers").toOptSeqFormat[BidResult]

        val impression_type: IMPRESSIONTYPE.TYPE = impression match {
          case x if x >= IMPRESSIONTYPE.UNKNOWN_IMPRESSION.id && x <= IMPRESSIONTYPE.EXTERNAL_CLK.id => { IMPRESSIONTYPE.apply(x) }
          case _ => { IMPRESSIONTYPE.UNKNOWN_IMPRESSION }
        }
        TradeLog(
          bid_id = bid_id,
          bid_timestamp = bid_timestamp,
          impression_type = impression_type,
          placement = placement,
          media = media,
          device = device,
          rate = rate,
          user = user,
          site = site,
          app = app,
          winner = winner,
          losers = losers
        )
      }
    }

    implicit object TradeImpressionFormat extends RootJsonFormat[TradeImpression] {
      def write(v: TradeImpression) = {
        val params = collection.mutable.ArrayBuffer.empty[JsField]

        params += "impression_id" -> JsString(v.impression_id)
        params += "impression_type" -> JsNumber(v.impression_type.id)
        params += "bid_timestamp" -> JsNumber(v.bid_timestamp)
        params += "bid_id" -> JsString(v.bid_id)
        params += "clear_price" -> JsString(v.clear_price)
        params += "dsp_id" -> JsString(v.dsp_id)
        params += "deal_type" -> JsNumber(v.deal_type.id)
        params += "placement" -> TradePlacementFormat.write(v.placement)
        params += "media" -> MediaFormat.write(v.media)
        params += "device" -> DeviceFormat.write(v.device)
        params += "rate" -> RateFormat.write(v.rate)
        if (v.user.isDefined) {
          params += "user" -> UserFormat.write(v.user.get)
        }
        if (v.site.isDefined) {
          params += "site" -> SiteFormat.write(v.site.get)
        }
        if (v.app.isDefined) {
          params += "app" -> AppFormat.write(v.app.get)
        }

        JsObject(params.toMap)
      }

      def read(value: JsValue) = {
        val fields = value.asJsObject.fields
        val impression_id = fields.checkExist("impression_id").strValue
        val impression = fields.checkExist("impression_type").intValue
        val bid_timestamp = fields.checkExist("bid_timestamp").longValue
        val bid_id = fields.checkExist("bid_id").strValue
        val clear_price = fields.checkExist("clear_price").strValue
        val dsp_id = fields.checkExist("dsp_id").strValue
        val deal = fields.checkExist("deal_type").intValue
        val placement = TradePlacementFormat.read(fields.checkExist("placement").get)
        val media = MediaFormat.read(fields.checkExist("media").get)
        val device = DeviceFormat.read(fields.checkExist("device").get)
        val rate = RateFormat.read(fields.checkExist("rate").get)
        val user = fields.get("user").optFormat[User]
        val site = fields.get("site").optFormat[Site]
        val app = fields.get("app").optFormat[App]

        val impression_type: IMPRESSIONTYPE.TYPE = impression match {
          case x if x >= IMPRESSIONTYPE.UNKNOWN_IMPRESSION.id && x <= IMPRESSIONTYPE.EXTERNAL_CLK.id => { IMPRESSIONTYPE.apply(x) }
          case _ => { IMPRESSIONTYPE.UNKNOWN_IMPRESSION }
        }
        val deal_type: DEAL_TYPE.TYPE = deal match {
          case x if x >= DEAL_TYPE.UNKNOWN_DEAL_TYPE.id && x <= DEAL_TYPE.CPA.id => { DEAL_TYPE.apply(x) }
          case _ => { DEAL_TYPE.UNKNOWN_DEAL_TYPE }
        }

        TradeImpression(
          impression_id = impression_id,
          impression_type = impression_type,
          bid_id = bid_id,
          bid_timestamp = bid_timestamp,
          clear_price = clear_price,
          dsp_id = dsp_id,
          deal_type = deal_type,
          placement = placement,
          media = media,
          device = device,
          rate = rate,
          user = user,
          site = site,
          app = app
        )
      }
    }

    implicit object TradeClickFormat extends RootJsonFormat[TradeClick] {
      def write(v: TradeClick) = {
        val params = collection.mutable.ArrayBuffer.empty[JsField]

        params += "click_id" -> JsString(v.click_id)
        params += "impression_type" -> JsNumber(v.impression_type.id)
        params += "bid_timestamp" -> JsNumber(v.bid_timestamp)
        params += "bid_id" -> JsString(v.bid_id)
        params += "clear_price" -> JsString(v.clear_price)
        params += "dsp_id" -> JsString(v.dsp_id)
        params += "deal_type" -> JsNumber(v.deal_type.id)
        params += "placement" -> TradePlacementFormat.write(v.placement)
        params += "media" -> MediaFormat.write(v.media)
        params += "device" -> DeviceFormat.write(v.device)
        params += "rate" -> RateFormat.write(v.rate)
        if (v.user.isDefined) {
          params += "user" -> UserFormat.write(v.user.get)
        }
        if (v.site.isDefined) {
          params += "site" -> SiteFormat.write(v.site.get)
        }
        if (v.app.isDefined) {
          params += "app" -> AppFormat.write(v.app.get)
        }

        JsObject(params.toMap)
      }

      def read(value: JsValue) = {
        val fields = value.asJsObject.fields
        val click_id = fields.checkExist("click_id").strValue
        logger.debug(s"click_id = ${click_id}")
        val impression = fields.checkExist("impression_type").intValue
        logger.debug(s"impression = ${impression}")
        val bid_timestamp = fields.checkExist("bid_timestamp").longValue
        logger.debug(s"bid_timestamp = ${bid_timestamp}")
        val bid_id = fields.checkExist("bid_id").strValue
        logger.debug(s"bid_id = ${bid_id}")
        val clear_price = fields.checkExist("clear_price").strValue
        logger.debug(s"clear_price = ${clear_price}")
        val dsp_id = fields.checkExist("dsp_id").strValue
        logger.debug(s"dsp_id = ${dsp_id}")
        val deal = fields.checkExist("deal_type").intValue
        logger.debug(s"deal = ${deal}")
        val placement = TradePlacementFormat.read(fields.checkExist("placement").get)
        logger.debug(s"placement = ${placement}")
        val media = MediaFormat.read(fields.checkExist("media").get)
        logger.debug(s"media = ${media}")
        val device = DeviceFormat.read(fields.checkExist("device").get)
        logger.debug(s"device = ${device}")
        val rate = RateFormat.read(fields.checkExist("rate").get)
        logger.debug(s"rate = ${rate}")
        val user = fields.get("user").optFormat[User]
        logger.debug(s"user = ${user}")
        val site = fields.get("site").optFormat[Site]
        logger.debug(s"site = ${site}")
        val app = fields.get("app").optFormat[App]
        logger.debug(s"app = ${app}")

        val impression_type: IMPRESSIONTYPE.TYPE = impression match {
          case x if x >= IMPRESSIONTYPE.UNKNOWN_IMPRESSION.id && x <= IMPRESSIONTYPE.EXTERNAL_CLK.id => { IMPRESSIONTYPE.apply(x) }
          case _ => { IMPRESSIONTYPE.UNKNOWN_IMPRESSION }
        }
        logger.debug(s"impression_type = ${impression_type}")
        val deal_type: DEAL_TYPE.TYPE = deal match {
          case x if x >= DEAL_TYPE.UNKNOWN_DEAL_TYPE.id && x <= DEAL_TYPE.CPA.id => { DEAL_TYPE.apply(x) }
          case _ => { DEAL_TYPE.UNKNOWN_DEAL_TYPE }
        }
        logger.debug(s"deal_type = ${deal_type}")

        TradeClick(
          click_id = click_id,
          impression_type = impression_type,
          bid_id = bid_id,
          bid_timestamp = bid_timestamp,
          clear_price = clear_price,
          dsp_id = dsp_id,
          deal_type = deal_type,
          placement = placement,
          media = media,
          device = device,
          rate = rate,
          user = user,
          site = site,
          app = app
        )
      }
    }

    implicit object EdgeEventFormat extends RootJsonFormat[EdgeEvent] {
      def write(v: EdgeEvent) = {
        val params = collection.mutable.ArrayBuffer.empty[JsField]

        params += "event_type" -> JsNumber(v.event_type.id)
        params += "event_key" -> JsString(v.event_key)
        v.event_type match {
          case MESSAGETYPE.TRADELOG => params += "trade_log" -> TradeLogFormat.write(v.trade_log.get)
          case MESSAGETYPE.IMPRESSION => params += "impression" -> TradeImpressionFormat.write(v.impression.get)
          case MESSAGETYPE.CLICK => params += "click" -> TradeClickFormat.write(v.click.get)
        }

        JsObject(params.toMap)
      }

      def read(value: JsValue) = {
        val fields = value.asJsObject.fields
        val event = fields.checkExist("event_type").intValue
        val event_key = fields.checkExist("event_key").strValue

        logger.debug(s"event = $event")
        logger.debug(s"event_key = ${event_key}")

        val event_type: MESSAGETYPE.TYPE = event match {
          case x if x >= MESSAGETYPE.UNKNOWN_MESSAGETYPE.id && x <= MESSAGETYPE.CLICK.id => { MESSAGETYPE.apply(x) }
          case _ => { MESSAGETYPE.UNKNOWN_MESSAGETYPE }
        }

        logger.debug(s"event_type = ${event_type}")

        val (trade_log, impression, click) = event_type match {
          case MESSAGETYPE.TRADELOG => {
            val tradeLog = if (fields.contains("trade_log")) {
              fields.get("trade_log").get.convertTo[TradeLog]
            } else throw new DeserializationException("Not found trade_log field in TRADELOG event")
            logger.debug(s"tradeLog = $tradeLog")
            (Some(tradeLog), None, None)
          }
          case MESSAGETYPE.IMPRESSION => {
            val impression = if (fields.contains("impression")) {
              fields.get("impression").get.convertTo[TradeImpression]
            } else throw new DeserializationException("Not found impression field in IMPRESSION event")
            logger.debug(s"impression = $impression")
            (None, Some(impression), None)
          }
          case MESSAGETYPE.CLICK => {
            val click = if (fields.contains("click")) {
              logger.debug("fields contains click")
              val jsValue = fields.get("click").get
              logger.debug(s"jsValue = $jsValue")
              jsValue.convertTo[TradeClick]
            } else throw new DeserializationException("Not found click field in CLICK event")
            logger.debug(s"click = $click")
            (None, None, Some(click))
          }
        }

        logger.debug(s"trade_log = ${trade_log}")
        logger.debug(s"impression = ${impression}")
        logger.debug(s"click = ${click}")

        EdgeEvent(
          event_type = event_type,
          event_key = event_key,
          trade_log = trade_log,
          impression = impression,
          click = click
        )
      }
    }
  }
}
// scalastyle:on