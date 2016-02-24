package com.vpon.ssp.report.dedup.actor

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import scala.concurrent.{Promise, Future}
import scala.concurrent.ExecutionContext.Implicits.global

import com.amazonaws.auth.{AWSCredentialsProvider, DefaultAWSCredentialsProviderChain, AWSCredentials}
import com.amazonaws.event.{ProgressEventType, ProgressEvent, ProgressListener}
import com.amazonaws.services.s3.model.{PutObjectRequest, ObjectMetadata}
import com.amazonaws.services.s3.transfer.{Upload, TransferManager}
import org.joda.time.format.{DateTimeFormatter, DateTimeFormat}
import org.slf4j.LoggerFactory

import com.vpon.ssp.report.common.model.DruidRecord


case class AwsConfig (
   s3BucketName: String,
   redshiftDataDelimiter: Char,
   s3DateTimePattern: String,
   needCompress: Boolean,
   needEncrypt: Boolean)

class AwsS3Service(awsConfig: AwsConfig) {

  val logger = LoggerFactory.getLogger("AwsRedshiftS3Service")

  val DATA_PREFIX: String = "data/"

  val awsCredentialsProvider: AWSCredentialsProvider = new DefaultAWSCredentialsProviderChain()
  val credentials: AWSCredentials = awsCredentialsProvider.getCredentials

  val tx: TransferManager = new TransferManager(credentials)

  val s3 = tx.getAmazonS3Client()

  val s3BucketName: String = awsConfig.s3BucketName

  val redshiftDataDelimiter: Char = awsConfig.redshiftDataDelimiter

  val s3DateTimePattern: String = awsConfig.s3DateTimePattern
  val fmt: DateTimeFormatter = DateTimeFormat.forPattern(s3DateTimePattern).withZoneUTC()

  // TODO
  val needCompress: Boolean = awsConfig.needCompress
  val needEncrypt: Boolean = awsConfig.needEncrypt

  def putRecords(allRecords: List[DruidRecord]): Future[Int] = {

    val counts: Iterable[Future[Int]] = allRecords.groupBy(getS3Folder(_)).map(kv => {
      val s3Folder = kv._1
      val groupedRecords = kv._2.sortBy(_.timestamp)
      // Write all of the records to a compressed output stream
      val baos: ByteArrayOutputStream = new ByteArrayOutputStream
      for (record <- groupedRecords) {
        val data = record.toDelimitedString(redshiftDataDelimiter)
        println("data ===> " + data)
        try {
          baos.write(data.getBytes)
        }
        catch {
          case e: Exception => {
            logger.error("Error writing record to output stream. Failing this emit attempt. Record: " + data, e)
          }
        }
      }

      val p = Promise[Boolean]()
      val progressListener = new ProgressListener() {
        def progressChanged(progressEvent: ProgressEvent) {
          progressEvent.getEventType match {
            case ProgressEventType.TRANSFER_COMPLETED_EVENT => {
              println("===> upload completed")
              p.success(true)
            }
            case ProgressEventType.TRANSFER_FAILED_EVENT => {
              println("===> upload failure")
              p.success(false)
            }
            case et @ _ => {
              println("===> met " + et)
            }
          }
        }
      }

      try {
        // Get the Amazon S3 filename
        val s3FileName: String = getS3FileName(groupedRecords)
        val s3Object: ByteArrayInputStream = new ByteArrayInputStream(baos.toByteArray)
        val metadata = new ObjectMetadata
        val s3Key = s3Folder + s3FileName
        val putObjectRequest = new PutObjectRequest(s3BucketName, s3Key, s3Object, metadata)
        val upload: Upload = tx.upload(putObjectRequest)
        upload.addProgressListener(progressListener)
        p.future.map(r => r match {
          case true => {
            logger.info("Successfully emitted " + groupedRecords.size + " records to Amazon S3")
            groupedRecords.size
          }
          case false => {
            logger.info("Failed to emit " + groupedRecords.size + " records to Amazon S3")
            0
          }
        })
      }
      catch {
        case e: Exception => {
          logger.error("Caught exception when uploading file to Amazon S3. Failing this emit attempt.", e)
          Future(0)
        }
      }
    })

    Future.sequence(counts) map {a => a.reduceLeft(_ + _)}
  }

  private def getS3FileName(records: List[DruidRecord]): String = {
    records.head.event_key + "-" + records.last.event_key
  }

  private def getS3Folder(record: DruidRecord): String = {
    val dateStr = fmt.print(record.timestamp)
    DATA_PREFIX + dateStr
  }

}
