package com.vpon.ssp.report.replay

import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}


object Utils {

  val reqDateTimePattern = "yyyyMMddHHmm"
  val s3DateTimePattern = "yyyy/MM/dd/HH/mm"

  val reqFmt: DateTimeFormatter = DateTimeFormat.forPattern(reqDateTimePattern).withZoneUTC()
  val s3Fmt: DateTimeFormatter = DateTimeFormat.forPattern(s3DateTimePattern).withZoneUTC()

  // convert from yyyyMMddHHmm to yyyy/MM/dd/HH/mm
  def convertTimeString(time: String): String = {
    val reqDt = reqFmt.parseDateTime(time)
    s3Fmt.print(reqDt)
  }

}
