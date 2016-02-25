// scalastyle:off
package com.vpon.ssp.report.dedup.generator


import org.scalacheck.Gen

import com.vpon.ssp.report.dedup.model._

object SupportingDataGenerator {

  def genIdString: Gen[String] = {
    for {
      k <- Gen.uuid
    } yield {
      k.toString
    }
  }

  val genDealType: Gen[Seq[Int]] = {
    val allDealTypes: Seq[Int] = Seq(1, 2) // 1: CPM, 2: CPC
    Gen.someOf(allDealTypes)
  }

  val genPublisherRevenueShareType: Gen[Int] = {
    // 0: NO_PAYMENT; 1: CPM; 2: CPC; 3: CPA 4: OWNER_CPM; 5: OWNER_REVENUE_SHARE
    // but currently we only use (4) and (5)
    Gen.oneOf(4, 5)
  }

  val genState: Gen[Int] = {
    // 0: Active; 1: Inactive; 2: Deleted
    Gen.oneOf(0, 1)
  }

  def genPublisherRevenueShare(publisherRevenueShareTypeId: Int): Gen[BigDecimal] = {
    val publisherRevenueShareType = PublisherRevenueShareType(publisherRevenueShareTypeId)
    publisherRevenueShareType match {
      case PublisherRevenueShareType.OWNER_CPM => Gen.choose(50d, 300d).map(v => BigDecimal(v))
      case PublisherRevenueShareType.OWNER_REVENUE_SHARE => Gen.choose(0.01d, 0.99d).map(v => BigDecimal(v))
    }
  }

  val genPlatformId: Gen[String] = Gen.const("1")

  val genCountryId: Gen[String] = Gen.oneOf("JP", "CN", "TW", "HK", "US")

  val genCurrency: Gen[String] = Gen.oneOf("JPY", "CNY", "TWD", "HKD", "USD")

  val genAdType: Gen[Int] = Gen.const(1) // 1: interstitial

  val genUcat: Gen[Seq[Int]] = {
    val allUcats: Seq[Int] = for(i <- 1 to 10) yield i
    Gen.someOf(allUcats)
  }

  val genTaxRate: Gen[BigDecimal] = Gen.choose(0d, 0.399d).map(v => BigDecimal(v))

  val genSellerRevenueShareType = Gen.oneOf(0, 1) // 0: percent; 1: fixed cpm

  def genSellerRevenueShare(sellerRevenueShareType: Int): Gen[BigDecimal] = {
    sellerRevenueShareType match {
      case 0 => Gen.choose(0d, 0.69d).map(v => BigDecimal(v))
      case 1 => Gen.choose(50d, 100d).map(v => BigDecimal(v))
    }
  }

  def genBidFloor(min: Double, max: Double): Gen[BigDecimal] = {
    Gen.choose(min, max).map(v => BigDecimal(v))
  }

  val genPublisherObject: Gen[PublisherObject] = for {
    publisher_revenue_share_type <- genPublisherRevenueShareType
    publisher_revenue_share <- genPublisherRevenueShare(publisher_revenue_share_type)
    country_id <- genCountryId
    currency <- genCurrency
    is_cpm_apply_default <- Gen.oneOf(true, false)
  } yield new PublisherObject(publisher_revenue_share_type, publisher_revenue_share, country_id, currency, is_cpm_apply_default)

  def genPlacementObject(publisher_Id: String): Gen[PlacementObject] = for {
    group_id <- genIdString
    ucat <- genUcat
  } yield new PlacementObject(group_id, publisher_Id, ucat.toList.sorted)

  def genExchangeRateObject(minFromUsdRate: Double, maxFromUsdRate: Double): Gen[ExchangeRateObject] = for {
    from_usd_rate <- Gen.choose(minFromUsdRate, maxFromUsdRate).map(v => BigDecimal(v).setScale(6, BigDecimal.RoundingMode.HALF_UP))
    to_usd_rate = (BigDecimal(1) / from_usd_rate).setScale(6, BigDecimal.RoundingMode.HALF_UP)
  } yield new ExchangeRateObject(from_usd_rate, to_usd_rate)

}
// scalastyle:on
