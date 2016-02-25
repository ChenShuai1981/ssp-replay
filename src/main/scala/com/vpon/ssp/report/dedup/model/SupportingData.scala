// scalastyle:off
package com.vpon.ssp.report.dedup.model

import scala.math.BigDecimal

import spray.json.DefaultJsonProtocol

object PublisherRevenueShareType extends Enumeration {
  type EnumVal = Value

  val NO_PAYMENT = Value(0, "NO_PAYMENT")
  val CPM = Value(1, "CPM")
  val CPC = Value(2, "CPC")
  val CPA = Value(3, "CPA")
  val OWNER_CPM = Value(4, "OWNER_CPM")
  val OWNER_REVENUE_SHARE = Value(5, "OWNER_REVENUE_SHARE")
}

object SellerRevenueShareType extends Enumeration {
  type SellerRevenueShareType = Value

  val PERCENT = Value(0, "PERCENT")
  val FIXED_CPM = Value(1, "FIXED_CPM")
}

object CurrencyEnum extends Enumeration {
  type id = Value
  val CNY = Value("CNY")
  val JPY = Value("JPY")
  val HKD = Value("HKD")
  val TWD = Value("TWD")
}

case class PlacementObject(
                            group_id: String,
                            publisher_id: String,
                            ucat: Seq[Int])

case class  PublisherObject(
                             publisher_revenue_share_type: Int,
                             publisher_revenue_share: BigDecimal,
                             country_id: String,
                             currency: String,
                             is_cpm_apply_default: Boolean)

case class ExchangeRateObject(
                               from_usd_rate: BigDecimal,
                               to_usd_rate: BigDecimal)

case class DspSspTaxRateObject(
                                pre_vat_1: BigDecimal,
                                pre_vat_2: BigDecimal,
                                pre_wht_1: BigDecimal,
                                pre_wht_2: BigDecimal,
                                exchange_rate_markup: BigDecimal,
                                seller_revenue_share_type: Int, // 0: percent; 1: fixed cpm
                                seller_revenue_share: BigDecimal,
                                post_vat_1: BigDecimal,
                                post_vat_2: BigDecimal,
                                post_wht_1: BigDecimal,
                                post_wht_2: BigDecimal)

case class PublisherSspTaxRateObject(
                                      post_seller_vat_1: BigDecimal,
                                      post_seller_wht_1: BigDecimal)

object PlacementObjectJsonProtocol extends DefaultJsonProtocol {
  implicit val placementObjectFormat = jsonFormat3(PlacementObject)
}

object PublisherObjectJsonProtocol extends DefaultJsonProtocol {
  implicit val publisherObjectFormat = jsonFormat5(PublisherObject)
}


object ExchangeRateObjectJsonProtocol extends DefaultJsonProtocol {
  implicit val exchangeRateFormat = jsonFormat2(ExchangeRateObject)
}

object PublisherSspTaxRateObjectJsonProtocol extends DefaultJsonProtocol {
  implicit val publisherSspTaxRateFormat = jsonFormat2(PublisherSspTaxRateObject)
}

object DspSspTaxRateObjectJsonProtocol extends DefaultJsonProtocol {
  implicit val dspSspTaxRateFormat = jsonFormat11(DspSspTaxRateObject)
}

// scalastyle:on
