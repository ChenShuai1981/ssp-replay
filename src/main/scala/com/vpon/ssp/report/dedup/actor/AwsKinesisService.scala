package com.vpon.ssp.report.dedup.actor

import java.nio.ByteBuffer
import scala.concurrent.{Promise, Future}

import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient
import com.amazonaws.services.kinesis.model.{ResourceNotFoundException, DescribeStreamResult, PutRecordResult, PutRecordRequest}
import org.slf4j.LoggerFactory


case class AwsKinesisConfig(streamName: String)

class AwsKinesisService(awsKinesisConfig: AwsKinesisConfig) {

  val logger = LoggerFactory.getLogger("AwsKinesisService")

  val kinesisAsyncClient = new AmazonKinesisAsyncClient

  def sendMessageToKinesis(payload: ByteBuffer, partitionKey: String): Future[Boolean] = {

    try{
      validateStream(awsKinesisConfig.streamName);
    } catch {
      case e: ResourceNotFoundException => logger.error(e.getMessage())
    }

    val putRecordRequest = new PutRecordRequest()
    putRecordRequest.setStreamName(awsKinesisConfig.streamName)
    putRecordRequest.setPartitionKey(partitionKey)
    putRecordRequest.setData(payload)

    logger.info("Writing to streamName " + awsKinesisConfig.streamName + " using partitionkey " + partitionKey)

    val p = Promise[Boolean]()

    kinesisAsyncClient.putRecordAsync(putRecordRequest,
      new AsyncHandler[PutRecordRequest, PutRecordResult]() {
        @Override
        def onError(e: Exception) {
          logger.error("Can't publish the record")
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

  private def validateStream(streamName: String) {
    val describeStreamResult: DescribeStreamResult = kinesisAsyncClient.describeStream(streamName)
    if(!describeStreamResult.getStreamDescription().getStreamStatus().equalsIgnoreCase("ACTIVE")){
      throw new ResourceNotFoundException("stream not found in an 'active' stage");
    }
  }
}
