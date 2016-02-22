package com.vpon.ssp.report.dedup.flatten.exception

class CouchbaseException(msg:String) extends RuntimeException(msg)
class DelayedException(msg:String) extends RuntimeException(msg)
class InvalidEventException(msg:String) extends RuntimeException(msg)

class PlacementObjectNotFoundException(msg:String) extends RuntimeException(msg)
class PublisherObjectNotFoundException(msg:String) extends RuntimeException(msg)

class ExchangeRateObjectNotFoundException(msg:String) extends RuntimeException(msg)
class PublisherSspTaxRateObjectNotFoundException(msg:String) extends RuntimeException(msg)
class DspSspTaxRateObjectNotFoundException(msg:String) extends RuntimeException(msg)

class SecretKeyNotFoundException(msg:String) extends RuntimeException(msg)
class DecryptClearPriceException(msg:String) extends RuntimeException(msg)

class CouchbaseDeserializationException(msg:String) extends RuntimeException(msg)
class UnsupportedPublisherRevenueShareTypeException(msg:String) extends RuntimeException(msg)
class UnsupportedSellerRevenueShareTypeException(msg:String) extends RuntimeException(msg)
class UnsupportedDealTypeException(msg:String) extends RuntimeException(msg)

class UnknownEventTypeException(msg:String) extends RuntimeException(msg)
