package com.vpon.ssp.report.dedup.model

import scala.collection.mutable.StringBuilder
import scala.math._
import com.vpon.ssp.report.edge.JsonHelper
import JsonHelper._
import spray.json._


case class EventRecord (
   appId: String = "",
   bidFloor: BigDecimal,
   bidId: String = "",
   buyerGroupId: String = "",
   buyerId: String = "",
   buyerType: Int,
   carrierId: Int,
   clearPrice: BigDecimal,
   connectionTypeId: Int,
   contentCategory: Seq[Int],
   dealType: Int,
   deviceMake: String = "",
   deviceModel: String = "",
   deviceOs: Seq[Int],
   deviceType: Int,
   dspSspTaxRateVersion: Int,
   eventKey: String = "",
   eventTime: Long,
   eventType: Int,
   exchangeRateVersion: Int,
   geoIds: Seq[Int],
   impressionType: Int,
   isCPMApplyDefault: Boolean,
   languageId: Int,
   mediaSize: String = "",
   mediaType: Int,
   placementGroupId: String = "",
   placementId: String = "",
   publisherCountryCode: String = "",
   publisherId: String = "",
   publisherRevenueShare: BigDecimal,
   publisherRevenueShareType: Int,
   publisherSspTaxRateVersion: Int,
   screenDensity: Int,
   sellerId: String = "",
   sellerType: Int,
   siteDomain: String = "",
   siteProtocol: Int,
   supplyType: Int,
   userAge: Int,
   userGender: Int) {

  def toDelimitedString(delim: Char = '|') :String = {
    import com.vpon.ssp.report.dedup.util.TimeUtil._

    val contentCategoryString: String = contentCategory.mkString(",")
    val deviceOsString: String = deviceOs.mkString(",")
    val geoIdsString: String = geoIds.mkString(",")
    val eventTimeString: String = convertTimestampToString(eventTime)

    val b: StringBuilder = new StringBuilder()
    b.append(appId).append(delim)
      .append(bidFloor).append(delim)
      .append(bidId).append(delim)
      .append(buyerGroupId).append(delim)
      .append(buyerId).append(delim)
      .append(buyerType).append(delim)
      .append(carrierId).append(delim)
      .append(clearPrice).append(delim)
      .append(connectionTypeId).append(delim)
      .append(contentCategoryString).append(delim)
      .append(dealType).append(delim)
      .append(deviceMake).append(delim)
      .append(deviceModel).append(delim)
      .append(deviceOsString).append(delim)
      .append(deviceType).append(delim)
      .append(dspSspTaxRateVersion).append(delim)
      .append(eventType).append(delim)
      .append(exchangeRateVersion).append(delim)
      .append(geoIdsString).append(delim)
      .append(impressionType).append(delim)
      .append(isCPMApplyDefault).append(delim)
      .append(languageId).append(delim)
      .append(mediaSize).append(delim)
      .append(mediaType).append(delim)
      .append(placementGroupId).append(delim)
      .append(placementId).append(delim)
      .append(publisherCountryCode).append(delim)
      .append(publisherId).append(delim)
      .append(publisherRevenueShare).append(delim)
      .append(publisherRevenueShareType).append(delim)
      .append(publisherSspTaxRateVersion).append(delim)
      .append(screenDensity).append(delim)
      .append(sellerId).append(delim)
      .append(sellerType).append(delim)
      .append(siteDomain).append(delim)
      .append(siteProtocol).append(delim)
      .append(supplyType).append(delim)
      .append(eventTimeString).append(delim)
      .append(userAge).append(delim)
      .append(userGender).append(delim)
      .append("\n")
    b.toString
  }
}

object EventRecordJsonProtocol extends DefaultJsonProtocol {

  implicit object EventRecordFormat extends RootJsonFormat[EventRecord] {
    def write(v: EventRecord) = {
      val params = collection.mutable.ArrayBuffer.empty[JsField]

      params += "appId" -> JsString(v.appId)
      params += "bidFloor" -> JsNumber(v.bidFloor)
      params += "bidId" -> JsString(v.bidId)
      params += "buyerGroupId" -> JsString(v.buyerGroupId)
      params += "buyerId" -> JsString(v.buyerId)
      params += "buyerType" -> JsNumber(v.buyerType)
      params += "carrierId" -> JsNumber(v.carrierId)
      params += "clearPrice" -> JsNumber(v.clearPrice)
      params += "connectionTypeId" -> JsNumber(v.connectionTypeId)
      params += "contentCategory" -> JsArray(v.contentCategory.map(c => JsNumber(c)).toVector)
      params += "dealType" -> JsNumber(v.dealType)
      params += "deviceMake" -> JsNumber(v.deviceMake)
      params += "deviceModel" -> JsNumber(v.deviceModel)
      params += "deviceOs" -> JsArray(v.deviceOs.map(c => JsNumber(c)).toVector)
      params += "deviceType" -> JsNumber(v.deviceType)
      params += "dspSspTaxRateVersion" -> JsNumber(v.dspSspTaxRateVersion)
      params += "eventKey" -> JsString(v.eventKey)
      params += "eventTime" -> JsNumber(v.eventTime)
      params += "eventType" -> JsNumber(v.eventType)
      params += "exchangeRateVersion" -> JsNumber(v.exchangeRateVersion)
      params += "geoIds" -> JsArray(v.geoIds.map(c => JsNumber(c)).toVector)
      params += "impressionType" -> JsNumber(v.impressionType)
      params += "isCPMApplyDefault" -> JsBoolean(v.isCPMApplyDefault)
      params += "languageId" -> JsNumber(v.languageId)
      params += "mediaSize" -> JsString(v.mediaSize)
      params += "mediaType" -> JsNumber(v.mediaType)
      params += "placementGroupId" -> JsString(v.placementGroupId)
      params += "placementId" -> JsString(v.placementId)
      params += "publisherCountryCode" -> JsString(v.publisherCountryCode)
      params += "publisherId" -> JsString(v.publisherId)
      params += "publisherRevenueShare" -> JsNumber(v.publisherRevenueShare)
      params += "publisherRevenueShareType" -> JsNumber(v.publisherRevenueShareType)
      params += "publisherSspTaxRateVersion" -> JsNumber(v.publisherSspTaxRateVersion)
      params += "screenDensity" -> JsNumber(v.screenDensity)
      params += "sellerId" -> JsString(v.sellerId)
      params += "sellerType" -> JsNumber(v.sellerType)
      params += "siteDomain" -> JsString(v.siteDomain)
      params += "siteProtocol" -> JsNumber(v.siteProtocol)
      params += "supplyType" -> JsNumber(v.supplyType)
      params += "userAge" -> JsNumber(v.userAge)
      params += "userGender" -> JsNumber(v.userGender)

      JsObject(params.toMap)
    }

    def read(value: JsValue) = {

      class SeqIntReader extends JsonReader[Seq[Int]] {
        def read(json : spray.json.JsValue) : Seq[Int] = {
          json match {
            case v: JsArray => v.elements.map(_.toString().toInt)
            case _ => Seq.empty[Int]
          }
        }
      }

      implicit val seqIntReader = new SeqIntReader

      val fields = value.asJsObject.fields

      val appId = fields.checkExist("appId").strValue
      val bidFloor = fields.checkExist("bidFloor").decimalValue
      val bidId = fields.checkExist("bidId").strValue
      val buyerGroupId = fields.checkExist("buyerGroupId").strValue
      val buyerId = fields.checkExist("buyerId").strValue
      val buyerType = fields.checkExist("buyerType").intValue
      val carrierId = fields.checkExist("carrierId").intValue
      val clearPrice = fields.checkExist("clearPrice").decimalValue
      val connectionTypeId = fields.checkExist("connectionTypeId").intValue
      val contentCategory = fields.checkExist("contentCategory").toSeqFormat(seqIntReader)
      val dealType = fields.checkExist("dealType").intValue
      val deviceMake = fields.checkExist("deviceMake").strValue
      val deviceModel = fields.checkExist("deviceModel").strValue
      val deviceOs = fields.checkExist("deviceOs").toSeqFormat(seqIntReader)
      val deviceType = fields.checkExist("deviceType").intValue
      val dspSspTaxRateVersion = fields.checkExist("dspSspTaxRateVersion").intValue
      val eventKey = fields.checkExist("eventKey").strValue
      val eventTime = fields.checkExist("eventTime").longValue
      val eventType = fields.checkExist("eventType").intValue
      val exchangeRateVersion = fields.checkExist("exchangeRateVersion").intValue
      val geoIds = fields.checkExist("geoIds").toSeqFormat(seqIntReader)
      val impressionType = fields.checkExist("impressionType").intValue
      val isCPMApplyDefault = fields.checkExist("isCPMApplyDefault").boolValue
      val languageId = fields.checkExist("languageId").intValue
      val mediaSize = fields.checkExist("mediaSize").strValue
      val mediaType = fields.checkExist("mediaType").intValue
      val placementGroupId = fields.checkExist("placementGroupId").strValue
      val placementId = fields.checkExist("placementId").strValue
      val publisherCountryCode = fields.checkExist("publisherCountryCode").strValue
      val publisherId = fields.checkExist("publisherId").strValue
      val publisherRevenueShare = fields.checkExist("publisherRevenueShare").intValue
      val publisherRevenueShareType = fields.checkExist("publisherRevenueShareType").intValue
      val publisherSspTaxRateVersion = fields.checkExist("publisherSspTaxRateVersion").intValue
      val screenDensity = fields.checkExist("screenDensity").intValue
      val sellerId = fields.checkExist("sellerId").strValue
      val sellerType = fields.checkExist("sellerType").intValue
      val siteDomain = fields.checkExist("siteDomain").strValue
      val siteProtocol = fields.checkExist("siteProtocol").intValue
      val supplyType = fields.checkExist("supplyType").intValue
      val userAge = fields.checkExist("userAge").intValue
      val userGender = fields.checkExist("userGender").intValue

      EventRecord(
        appId = appId,
        bidFloor = bidFloor,
        bidId = bidId,
        buyerGroupId = buyerGroupId,
        buyerId = buyerId,
        buyerType = buyerType,
        carrierId = carrierId,
        clearPrice = clearPrice,
        connectionTypeId = connectionTypeId,
        contentCategory = contentCategory,
        dealType = dealType,
        deviceMake = deviceMake,
        deviceModel = deviceModel,
        deviceOs = deviceOs,
        deviceType = deviceType,
        dspSspTaxRateVersion = dspSspTaxRateVersion,
        eventKey = eventKey,
        eventTime = eventTime,
        eventType = eventType,
        exchangeRateVersion = exchangeRateVersion,
        geoIds = geoIds,
        impressionType = impressionType,
        isCPMApplyDefault = isCPMApplyDefault,
        languageId = languageId,
        mediaSize = mediaSize,
        mediaType = mediaType,
        placementGroupId = placementGroupId,
        placementId = placementId,
        publisherCountryCode = publisherCountryCode,
        publisherId = publisherId,
        publisherRevenueShare = publisherRevenueShare,
        publisherRevenueShareType = publisherRevenueShareType,
        publisherSspTaxRateVersion = publisherSspTaxRateVersion,
        screenDensity = screenDensity,
        sellerId = sellerId,
        sellerType = sellerType,
        siteDomain = siteDomain,
        siteProtocol = siteProtocol,
        supplyType = supplyType,
        userAge = userAge,
        userGender = userGender
      )
    }
  }

}
