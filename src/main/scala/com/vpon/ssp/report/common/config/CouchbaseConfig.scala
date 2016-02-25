package com.vpon.ssp.report.common.config

import scala.collection.JavaConversions._
import scala.language.postfixOps
import scala.util.Try

import com.couchbase.client.java.CouchbaseCluster

import com.vpon.ssp.report.dedup.couchbase.BucketInfo

trait CouchbaseConfig extends BaseConfig {

  lazy val couchbaseConnectionString: String = config.getString("couchbase.connection-string")
  lazy val couchbaseCluster = CouchbaseCluster.fromConnectionString(couchbaseConnectionString)
  lazy val bucketsMap: Map[String, BucketInfo] = config.getObject("couchbase.buckets").toMap map {
    case (key, value) =>
      val bucketConfig = value.asInstanceOf[com.typesafe.config.ConfigObject].toConfig
      val name = Try { bucketConfig.getString("name") } getOrElse key
      val password = Try { bucketConfig.getString("password") } getOrElse ""
      val keyPrefix = Try { bucketConfig.getString("key-prefix") } getOrElse ""
      (key, BucketInfo(name, password, keyPrefix))
  }

}
