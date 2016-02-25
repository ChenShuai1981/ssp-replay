package com.vpon.ssp.report.common.config

import com.typesafe.config.ConfigFactory

trait BaseConfig {

  val config = ConfigFactory.load()

}
