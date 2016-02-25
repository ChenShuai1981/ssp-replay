//package com.vpon.ssp.report.common.config
//
//import java.util.concurrent.TimeUnit
//import scala.language.postfixOps
//import scala.concurrent.duration._
//
//trait ClusterConfig extends BaseConfig {
//  lazy val clusterGroup: String = config.getString("app.cluster-group")
//  lazy val clusterRampupTime: FiniteDuration = config.getDuration("app.cluster-rampup-time", TimeUnit.SECONDS).seconds
//}
