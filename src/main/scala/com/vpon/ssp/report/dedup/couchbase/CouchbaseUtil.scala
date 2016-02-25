package com.vpon.ssp.report.dedup.couchbase

import scala.collection.JavaConversions._

import com.couchbase.client.java.Bucket
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.error.DocumentAlreadyExistsException
import com.couchbase.client.java.view._
import org.apache.commons.lang3.exception.ExceptionUtils
import org.slf4j.LoggerFactory

import com.vpon.ssp.report.dedup.flatten.exception.CouchbaseException


case class ExchangeRateObjectKey(version: Int,
                                 currency: String,
                                 keyPrefix: String = "") {
  def docKey: String = s"${keyPrefix}exchange_rate_${currency}_${version}"
}

case class PublisherSspTaxRateObjectKey(version: Int,
                                        publisherCountryCode: String,
                                        platformId: String,
                                        keyPrefix: String = "") {
  def docKey: String = s"${keyPrefix}publisher_ssp_tax_rate_${platformId}_${publisherCountryCode}_${version}"
}

case class DspSspTaxRateObjectKey(version: Int,
                                  dspId: String,
                                  platformId: String,
                                  keyPrefix: String = "") {
  def docKey: String = s"${keyPrefix}dsp_ssp_tax_rate_${platformId}_${dspId}_${version}"
}

case class PublisherObjectKey(publisherId: String,
                              platformId: String,
                              keyPrefix: String = "") {
  def docKey: String = s"${keyPrefix}publishers_${platformId}_${publisherId}"
}

case class PlacementObjectKey(placementId: String,
                              platformId: String,
                              keyPrefix: String = "") {
  def docKey: String = s"${keyPrefix}placements_${platformId}_${placementId}"
}

object CouchbaseUtil {

  val log = LoggerFactory.getLogger(CouchbaseUtil.getClass)

  def upsertData(key: String, json: String, bucket: Bucket): JsonDocument = {
    val content = JsonObject.fromJson(json)
    val doc = JsonDocument.create(s"$key", content)
    try {
      bucket.upsert(doc)
    } catch {
      case e: Throwable => {
        log.error(ExceptionUtils.getStackTrace(e))
        throw new CouchbaseException("Couchbase error")
      }
    }
  }

  def getData(key: String, bucket: Bucket): JsonDocument = {
    try {
      bucket.get(key)
    } catch {
      case e: Throwable => throw new CouchbaseException("Couchbase error")
    }
  }

  def removeData(key: String, bucket: Bucket): JsonDocument = {
    try {
      bucket.remove(key)
    } catch {
      case e: Throwable => throw new CouchbaseException("Couchbase error")
    }
  }

  def removeAllDocs(bucket: Bucket) {
    val designDocumentName = "bucket"
    val viewName = "all_keys"
    def queryAndRemoveAll(): Unit = {
      doQueryAndRemoveAll()
      doQueryAndRemoveAll()
    }
    def doQueryAndRemoveAll() {
      val viewResult = bucket.query(ViewQuery.from(designDocumentName, viewName))
      viewResult.allRows().foreach { row =>
        val key = row.key.asInstanceOf[String]
        try {
          bucket.remove(key)
        } catch {
          case e: Throwable => log.debug(s"deleting ${key}")
        }
      }
    }
    def createDesignDocument = {
      val views = new java.util.ArrayList[com.couchbase.client.java.view.View]()
      val designDoc = bucket.bucketManager.insertDesignDocument(DesignDocument.create(designDocumentName, views))
      createView(designDoc)
    }
    def createView(designDoc: DesignDocument) = {
      val mapFunction = """function (doc, meta) { emit(meta.id, null); }"""
      val viewDesign = DefaultView.create(viewName, mapFunction)
      designDoc.views.add(viewDesign)
      bucket.bucketManager.upsertDesignDocument(designDoc)
    }
    try {
      val optDesignDoc = Option(bucket.bucketManager.getDesignDocument(designDocumentName))
      optDesignDoc match {
        case None => createDesignDocument
        case Some(designDoc) => {
          val viewNames = designDoc.views map (v => v.name)
          if (!viewNames.contains(viewName)) {
            createView(designDoc)
          }
        }
      }
    } catch {
      case e: Exception => {
        createDesignDocument
      }
    }
    // in case VIEW doesn't get all keys at first time, call it twice
    queryAndRemoveAll()
    queryAndRemoveAll()
  }
}
