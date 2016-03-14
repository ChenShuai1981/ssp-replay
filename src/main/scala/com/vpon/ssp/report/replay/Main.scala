package com.vpon.ssp.report.replay


object Main extends App {

  println("Please input replay start time: [yyyyMMddHHmm]")
  val startTimeStr = scala.io.StdIn.readLine

  println()

  println("Please input replay end time: [yyyyMMddHHmm]")
  val endTimeStr = scala.io.StdIn.readLine

  println()

  val startTime = safeStringToLong(startTimeStr)
  if (!startTime.isDefined) {
    println(s"$startTimeStr is NOT a Long number!")
  }
  val endTime = safeStringToLong(endTimeStr)
  if (!endTime.isDefined) {
    println(s"$endTimeStr is NOT a Long number!")
  }

  println("Replay now ......")

  ReplayService.replay(startTime.get, endTime.get)

  def safeStringToLong(str: String): Option[Long] = try {
    Some(str.toLong)
  } catch {
    case e:NumberFormatException => None
  }

}
