package com.vpon.ssp.report.dedup.util

import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

import com.vpon.ssp.report.dedup.model.EventRecord


object S3Util {

  val s3DateTimePattern = "yyyy/MM/dd/HH/mm/"

  val fmt: DateTimeFormatter = DateTimeFormat.forPattern(s3DateTimePattern).withZoneUTC()

  def getS3FileName(records: List[EventRecord], partitionId: Option[Int]): String = {
    partitionId match {
      case Some(pid) => pid + "*" + records.size + "*" + records.head.eventKey + "*" + records.last.eventKey
      case None => records.head.eventKey + "*" + records.last.eventKey
    }
  }

  def getS3Folder(record: EventRecord, prefix: String): String = {
    val dateStr = fmt.print(record.eventTime)
    prefix + dateStr
  }

}
