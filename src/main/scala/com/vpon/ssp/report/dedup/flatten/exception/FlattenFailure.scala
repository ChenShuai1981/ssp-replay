package com.vpon.ssp.report.dedup.flatten.exception

case class FlattenFailure(message: String, errorType: FlattenFailureType.Value)

object FlattenFailureType extends Enumeration {
  type Failure = Value

  val MappingError = Value("mapping_error")
  val CouchbaseError = Value("couchbase_error")
  val UnknownError = Value("unknown_error")
  val CouchbaseDeserializationError = Value("couchbase_deserialization_error")
  val DelayedEvent = Value("delayed_event")
  val UnParsedEvent = Value("unparsed_event")
  val UnknownEventType = Value("unknown_event_type")
  val InvalidEvent = Value("invalid_event")
  val PlacementNotFound = Value("placement_not_found")
  val PublisherNotFound = Value("publisher_not_found")
  val ExchangeRateNotFound = Value("exchange_rate_not_found")
  val PublisherSspTaxRateNotFound = Value("publisher_ssp_tax_rate_not_found")
  val DspSspTaxRateNotFound = Value("dsp_ssp_tax_rate_not_found")
  val SecretKeyNotFound = Value("secret_key_not_found")
  val DecryptClearPriceError = Value("decript_clear_price_error")
  val UnsupportedPublisherRevenueShareType = Value("unsupported_publisher_revenue_share_type")
  val UnsupportedSellerRevenueShareType = Value("unsupported_seller_revenue_share_type")
  val UnsupportedDealType = Value("unsupported_deal_type")
}
