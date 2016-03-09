package com.vpon.ssp.report.replay


object Main extends App {

//  print("Please input begin time (UTC) in yyyyMMddHHmm format: ")
//  val beginTimeString = scala.io.StdIn.readLine()
//  val beginTime = try {
//    beginTimeString.toLong
//  } catch {
//    case e: Exception => throw new IllegalArgumentException("Should be yyyyMMddHHmm format")
//  }
//
//  print("Please input end time (UTC) in yyyyMMddHHmm format: ")
//  val endTimeString = scala.io.StdIn.readLine()
//  val endTime = try {
//    endTimeString.toLong
//  } catch {
//    case e: Exception => throw new IllegalArgumentException("Should be yyyyMMddHHmm format")
//  }
//
//  if ( checkRange(beginTime) && checkRange(endTime) && endTime >= beginTime) {
//    ReplayService.replay(beginTime, endTime)
//  } else {
//    print("Please ensure beginTime <= endTime !!")
//  }
//
//  def checkRange(num: Long): Boolean = num >= 200001010000L && num <= 300001010000L

  ReplayService.replay(201603090539L, 201603090539L)

}
