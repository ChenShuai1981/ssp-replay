package com.vpon.ssp.report.dedup.actor

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.ByteBuffer

import scala.concurrent.{Promise, Future}
import scala.concurrent.ExecutionContext.Implicits.global

import com.amazonaws.auth.{BasicAWSCredentials, AWSCredentialsProvider, DefaultAWSCredentialsProviderChain, AWSCredentials}
import com.amazonaws.event.{ProgressEventType, ProgressEvent, ProgressListener}
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.regions.RegionUtils
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient
import com.amazonaws.services.kinesis.model.{DescribeStreamResult, PutRecordResult, PutRecordRequest, ResourceNotFoundException}
import com.amazonaws.services.s3.model.{PutObjectRequest, ObjectMetadata}
import com.amazonaws.services.s3.transfer.{Upload, TransferManager}
import org.apache.commons.lang3.exception.ExceptionUtils
import org.slf4j.LoggerFactory

import com.vpon.ssp.report.dedup.model.EventRecord
import com.vpon.ssp.report.dedup.util.S3Util


case class AwsConfig (
   regionName: String,
   s3BucketName: String,
   dataPrefix: String,
   needCompress: Boolean,
   needEncrypt: Boolean,
   kinesisStreamName: String
)

class AwsService(awsConfig: AwsConfig) {

  val logger = LoggerFactory.getLogger("AwsService")

  val dataPrefix: String = awsConfig.dataPrefix

  val awsCredentialsProvider: AWSCredentialsProvider = new DefaultAWSCredentialsProviderChain()
  val credentials: AWSCredentials = awsCredentialsProvider.getCredentials

  val regionName = awsConfig.regionName

  val tx: TransferManager = new TransferManager(credentials)

  val s3BucketName: String = awsConfig.s3BucketName

  validateBucket(s3BucketName)

  // TODO
  val needCompress: Boolean = awsConfig.needCompress
  val needEncrypt: Boolean = awsConfig.needEncrypt

  val kinesisAsyncClient = new AmazonKinesisAsyncClient(credentials)
  val region = RegionUtils.getRegion(regionName)
  kinesisAsyncClient.setRegion(region)

  val kinesisStreamName: String = awsConfig.kinesisStreamName

  validateStream(kinesisStreamName)

  def send(allRecords: List[EventRecord], partitionId: Option[Int] = None): Future[Int] = {

    val counts: Iterable[Future[Int]] = allRecords.groupBy(r => S3Util.getS3Folder(r, dataPrefix)).map(kv => {
      val s3Folder = kv._1
      val groupedRecords = kv._2.sortBy(_.eventTime)
      // Write all of the records to a compressed output stream
      val baos: ByteArrayOutputStream = new ByteArrayOutputStream
      for (record <- groupedRecords) {
        val data = record.toDelimitedString()
        logger.debug("data ===> " + data)
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
              logger.debug("===> upload completed")
              p.success(true)
            }
            case ProgressEventType.TRANSFER_FAILED_EVENT => {
              logger.debug("===> upload failure")
              p.success(false)
            }
            case et @ _ => {
              logger.debug("===> met " + et)
            }
          }
        }
      }

      val s3FileName: String = S3Util.getS3FileName(groupedRecords, partitionId)
      val s3Object: ByteArrayInputStream = new ByteArrayInputStream(baos.toByteArray)
      val metadata = new ObjectMetadata
      val s3Key = s3Folder + s3FileName
      val putObjectRequest = new PutObjectRequest(s3BucketName, s3Key, s3Object, metadata)
      val upload: Upload = tx.upload(putObjectRequest)
      upload.addProgressListener(progressListener)

      val s3Future = p.future
      val kinesisFuture = sendMessageToKinesis(ByteBuffer.wrap(s3Key.getBytes()), kinesisStreamName)

      val f = for {
        s3Result <- s3Future
        kinesisResult <- kinesisFuture if s3Result
      } yield {
        kinesisResult match {
          case true => {
            println("=====> Successfully send to kinesis stream")
            groupedRecords.size
          }
          case false => 0
        }
      }

      f.recover{
        case e: Throwable => 0
      }

    })

    Future.sequence(counts) map {a => a.reduceLeft(_ + _)}
  }

  def sendMessageToKinesis(payload: ByteBuffer, partitionKey: String): Future[Boolean] = {

    val putRecordRequest = new PutRecordRequest()
    putRecordRequest.setStreamName(kinesisStreamName)
    putRecordRequest.setPartitionKey(partitionKey)
    putRecordRequest.setData(payload)

    logger.info("Writing to kinesisStreamName " + kinesisStreamName + " using partitionkey " + partitionKey)

    val p = Promise[Boolean]()

    kinesisAsyncClient.putRecordAsync(putRecordRequest,
      new AsyncHandler[PutRecordRequest, PutRecordResult]() {
        @Override
        def onError(e: Exception) {
          logger.error("Can't publish the record " + ExceptionUtils.getStackTrace(e))
          p.success(false)
        }

        @Override
        def onSuccess(putRecordRequest: PutRecordRequest, putRecordResult: PutRecordResult) {
          logger.info("published successfully")
          p.success(true)
        }
      }
    )

    p.future
  }

  private def validateBucket(bucketName: String) {
    if (!tx.getAmazonS3Client.doesBucketExist(bucketName)) {
      throw new ResourceNotFoundException(s"bucket $bucketName does NOT exist")
    }
  }

  private def validateStream(streamName: String) {
    val describeStreamResult: DescribeStreamResult = kinesisAsyncClient.describeStream(streamName)
    println(s"=====> describeStreamResult: $describeStreamResult")
    if(!describeStreamResult.getStreamDescription().getStreamStatus().equalsIgnoreCase("ACTIVE")){
      throw new ResourceNotFoundException("stream not found in an 'active' stage")
    }
  }

}
