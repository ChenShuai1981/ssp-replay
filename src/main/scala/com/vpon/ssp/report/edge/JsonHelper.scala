// scalastyle:off
package com.vpon.ssp.report.edge

import spray.json._

object JsonHelper {

  implicit class JsonFieldExtractor(value: Map[String, JsValue]) {
    def checkExist(key: String): Option[JsValue] = value.get(key) remap {
      case None => throw new DeserializationException(s"field $key is required")
      case x => x
    }
  }

  implicit class JsValueConversion(value: Option[JsValue]) {
    final def remap[T](func: Option[JsValue] => T): T = {
      val data = value match {
        case null => None
        case None => None
        case Some(JsNull) => None
        case x => x
      }
      func(data)
    }

    def optBoolean: Option[Boolean] = value remap {
      case Some(v: JsBoolean) => Some(v.value)
      case Some(x) => throw new DeserializationException("Expected JsBoolean, but got " + x)
      case _ => None
    }

    def optBigDecimal: Option[BigDecimal] = value remap {
      case Some(v: JsNumber) => Some(v.value)
      case Some(v: JsString) => Some(BigDecimal(v.value))
      case Some(x) => throw new DeserializationException("Expected JsNumber/JsString, but got " + x)
      case _ => None
    }
    def decimalValue(defaultValue: BigDecimal = 0): BigDecimal = optBigDecimal.getOrElse(defaultValue)
    def decimalValue: BigDecimal = decimalValue(0)

    def optDouble: Option[Double] = value remap {
      case Some(v: JsNumber) => Some(v.value.toDouble)
      case Some(v: JsString) => Some(v.value.toDouble)
      case Some(x) => throw new DeserializationException("Expected JsNumber/JsString, but got " + x)
      case _ => None
    }
    def doubleValue(defaultValue: Double = 0): Double = optDouble.getOrElse(defaultValue)
    def doubleValue: Double = 0

    def optLong: Option[Long] = value remap {
      case Some(v: JsNumber) => Some(v.value.longValue())
      case Some(v: JsString) => Some(v.value.toLong)
      case Some(x) => throw new DeserializationException("Expected JsNumber/JsString, but got " + x)
      case _ => None
    }
    def longValue(defaultValue: Long = 0): Long = optLong.getOrElse(defaultValue)
    def longValue: Long = longValue(0)

    def optInt: Option[Int] = value remap {
      case Some(v: JsNumber) => Some(v.value.intValue())
      case Some(v: JsString) => Some(v.value.toInt)
      case Some(x) => throw new DeserializationException("Expected JsNumber/JsString, but got " + x)
      case _ => None
    }
    def intValue(defaultValue: Int = 0): Int = optInt.getOrElse(defaultValue)
    def intValue: Int = intValue(0)

    def optStr: Option[String] = value remap {
      case Some(v: JsString) => Some(v.value)
      case Some(x) => throw new DeserializationException("Expected String as JsString, but got " + x)
      case _ => None
    }
    def strValue(defaultValue: String = ""): String = optStr.getOrElse(defaultValue)
    def strValue: String = strValue("")

    def boolValue(defaultValue: Boolean = false): Boolean = optBoolean.getOrElse(defaultValue)
    def boolValue: Boolean = boolValue(false)

    def toSeqFormat[T](implicit reader: JsonReader[Seq[T]]): Seq[T] = value remap {
      case Some(v: JsArray) => reader.read(v)
      case Some(x) => throw new DeserializationException("Expected Collection as JsArray, but got " + x)
      case _ => Seq.empty[T]
    }

    def toOptSeqFormat[T](implicit reader: JsonReader[Seq[T]]): Option[Seq[T]] = value remap {
      case Some(v: JsArray) => Some(reader.read(v))
      case Some(x) => throw new DeserializationException("Expected Collection as JsArray, but got " + x)
      case _ => None
    }

    def toFormat[T](defaultValue: T)(implicit reader: JsonReader[T]): T = value remap {
      case Some(v: JsObject) => reader.read(v)
      case Some(x) => throw new DeserializationException("Expected Object as JsObject, but got " + x)
      case _ => defaultValue
    }

    def optFormat[T](implicit reader: JsonReader[T]): Option[T] = value remap {
      case Some(v: JsObject) => Some(reader.read(v))
      case Some(x) => throw new DeserializationException("Expected Object as JsObject, but got " + x)
      case _ => None
    }
  }
}
// scalastyle:on