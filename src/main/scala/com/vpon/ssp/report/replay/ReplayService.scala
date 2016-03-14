package com.vpon.ssp.report.replay

import java.util.Properties

import scala.annotation.tailrec
import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.regions.RegionUtils
import com.amazonaws.services.s3.model.{GetObjectRequest, ObjectListing, ListObjectsRequest, S3ObjectSummary}
import com.amazonaws.services.s3.{AmazonS3Client, AmazonS3}
import com.couchbase.client.java.CouchbaseCluster
import com.couchbase.client.java.document.StringDocument
import org.apache.commons.lang.SerializationUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.slf4j.LoggerFactory


case class TimeInMinuteAndOffset(timeInMinute: Long, offset: Long)

case class S3KeyObject(topicName: String, timeInMinute: Long, partitionId: Int, offset: Long, count: Int)
/**
 * at least once send
 * it will be consumed by dedup component
 * so not an issue
 */
object ReplayService extends ReplayConfig {

  val logger = LoggerFactory.getLogger("ReplayService")

  val s3Delimiter = "/"
  val seperator = "."

  val awsCredentialsProvider: AWSCredentialsProvider = new DefaultAWSCredentialsProviderChain()
  val credentials: AWSCredentials = awsCredentialsProvider.getCredentials

  logger.info(s"regionName = $regionName")
  val region = RegionUtils.getRegion(regionName)

  val s3: AmazonS3 = new AmazonS3Client(credentials)
  s3.setRegion(region)

  logger.info(s"kafkaReplayBrokers = $kafkaReplayBrokers")
  logger.info(s"kafkaReplayKeySerializer = $kafkaReplayKeySerializer")
  logger.info(s"kafkaReplayValueSerializer = $kafkaReplayValueSerializer")

  val kafkaProps = new Properties()
  kafkaProps.put("bootstrap.servers", kafkaReplayBrokers)
  kafkaProps.put("acks", "1")
  kafkaProps.put("key.serializer", kafkaReplayKeySerializer)
  kafkaProps.put("value.serializer", kafkaReplayValueSerializer)

  val kafkaProducer = new KafkaProducer[String, Array[Byte]](kafkaProps)

  logger.info(s"cbConnectionString = $cbConnectionString")
  logger.info(s"cbBucketName = $cbBucketName")
  logger.info(s"cbBucketPassword = $cbBucketPassword")
  logger.info(s"cbBucketKeyPrefix = $cbBucketKeyPrefix")

  val couchbaseCluster = CouchbaseCluster.fromConnectionString(cbConnectionString)
  val cbBucket = couchbaseCluster.openBucket(cbBucketName, cbBucketPassword)

  logger.info(s"archiveS3BucketName = $archiveS3BucketName")
  logger.info(s"archivePeriod = $archivePeriod")
  logger.info(s"archiveKafkaTopic = $archiveKafkaTopic")

  var startFrom: Long = 0
  var endWith: Long = 0
  val offsetMap: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map.empty[String, String]

  // yyyyMMddHHmm like 201603031524 ~ 201603031630
  def replay(begin: Long, end: Long): Unit = {
    startFrom = begin
    endWith = end
    logger.debug(s"startFrom = $startFrom")
    logger.debug(s"endWith = $endWith")
    val range = for(i <- startFrom to endWith) yield i
    val timeInMinutes: Iterable[(Long, String)] = range.map(r => (r, getPrefix(r)))
    val allMessages = timeInMinutes.map(timeInMinute => {
      handleMinute(timeInMinute._1, timeInMinute._2)
    })
    val total = if (allMessages.isEmpty) {
      0
    } else {
      // each minute
      allMessages.reduceLeft(_ + _)
    }
    println(s"Replay S3 data of bucket $archiveS3BucketName between $startFrom and $endWith to kafka $kafkaReplayBrokers with topic $kafkaReplayTopic done, total $total messages sent.")
  }

  private def handleMinute(timeInMinute: Long, pathToMinute: String): Int = {
    logger.debug(s"pathToMinute = $pathToMinute")
    val pathToPartitionSeq: Seq[String] = listSubFolders(archiveS3BucketName, pathToMinute)
    val messagesPerMinute = pathToPartitionSeq.map(pathToPartition => {
      val partitionId = getPartitionId(pathToPartition)
      logger.debug(s"partitionId = $partitionId")
      val partitionOffsetOption = getPartitionOffset(partitionId)
      partitionOffsetOption match {
        case Some(partitionOffset) => {
          val lastTimeInMinute = getLastTimeInMinute(partitionOffset)
          logger.debug(s"lastTimeInMinute = $lastTimeInMinute")
          timeInMinute.compare(lastTimeInMinute) match {
            case 0 => {
              // = filter current list
              handlePartition(pathToPartition, Some(partitionOffset))
            }
            case 1 => {
              // > all need
              handlePartition(pathToPartition, None)
            }
            case -1 => {
              // < skip all
              0
            }
          }
        }
        case None => {
          // start from beginning
          handlePartition(pathToPartition, None)
        }
      }
    })
    if (messagesPerMinute.isEmpty) {
      0
    } else {
      // each partitions
      messagesPerMinute.reduceLeft(_ + _)
    }
  }

  @tailrec
  private def recurHandle(current: ObjectListing, s3ObjectSummaryList: List[S3ObjectSummary], partitionId: Int): Int = {
    current.isTruncated() match {
      case false => {
        logger.debug(s"isTruncated: false, prefix: ${current.getPrefix}, partitionId: $partitionId, ObjectSummariesSize: ${s3ObjectSummaryList.size}")
        sendS3Objects(s3ObjectSummaryList, partitionId)
      }
      case true => {
        import scala.collection.JavaConversions._
        val currentS3ObjectSummaryList = current.getObjectSummaries.toList
        logger.debug(s"isTruncated: true, prefix: ${current.getPrefix}, partitionId: $partitionId, ObjectSummariesSize: ${currentS3ObjectSummaryList.size}")
        sendS3Objects(currentS3ObjectSummaryList, partitionId)
        recurHandle(s3.listNextBatchOfObjects(current), s3ObjectSummaryList ::: currentS3ObjectSummaryList, partitionId)
      }
    }
  }

  private def sendS3Objects(s3ObjectSummaryList: List[S3ObjectSummary], partitionId: Int): Int = {
    val cbPartitionOffsetKey = getCBPartitionOffsetKey(partitionId)
    val s3Keys = s3ObjectSummaryList.map(_.getKey).filterNot(_.endsWith(s3Delimiter))
    val results = s3Keys.flatMap(s3Key => {
      logger.debug(s"s3Key = $s3Key")
      readS3ObjectAsRecords(s3Key, partitionId).map(record => {
        kafkaProducer.send(record)
        saveCBPartitionOffset(cbPartitionOffsetKey, s3Key)
        1
      })
    })
    if (results.isEmpty) {
      0
    } else {
      results.reduceLeft(_ + _)
    }
  }

  private def readS3ObjectAsRecords(s3ObjectKey: String, partitionId: Int): Iterable[ProducerRecord[String, Array[Byte]]] = {
    logger.debug(s"read s3 object $s3ObjectKey")
    val s3Obj = s3.getObject(new GetObjectRequest(archiveS3BucketName, s3ObjectKey))
    val s3Content: Array[Byte] = Stream.continually(s3Obj.getObjectContent.read).takeWhile(_ != -1).map(_.toByte).toArray
    val fileSuffix = s3ObjectKey.substring(s3ObjectKey.lastIndexOf(seperator) + 1)
    val decompressedS3Content = fileSuffix match {
      case "gz" => GZIPDecompressor.decompressData(s3Content)
      case "bz2" => BZIP2Decompressor.decompressData(s3Content)
      case "lzo" => LZOPDecompressor.decompressData(s3Content)
      case _ => s3Content // uncompress
    }
    val batchArrayBytes: Array[(String, Array[Byte])] = SerializationUtils.deserialize(decompressedS3Content).asInstanceOf[Array[(String, Array[Byte])]]
    batchArrayBytes.map(pair => {
      logger.debug(s"send to $kafkaReplayTopic with partition $partitionId and key ${pair._1}")
      new ProducerRecord(kafkaReplayTopic, partitionId, pair._1, pair._2)
    })
  }

  /**
   * <- topics/ssp-edge-events/minutely/2016/03/04/07/00
   * ->
   * topics/ssp-edge-events/minutely/2016/03/04/07/00/0/
   * topics/ssp-edge-events/minutely/2016/03/04/07/00/1/
   */
  private def listSubFolders(bucketName: String, prefix: String): Seq[String] = {
    val thePrefix = prefix.endsWith(s3Delimiter) match {
      case true => prefix
      case false => prefix + s3Delimiter
    }
    val listObjectsRequest: ListObjectsRequest = new ListObjectsRequest()
      .withBucketName(bucketName).withPrefix(thePrefix)
      .withDelimiter(s3Delimiter)
    val objects: ObjectListing = s3.listObjects(listObjectsRequest)
    import scala.collection.JavaConversions._
    val commonPrefixes = objects.getCommonPrefixes()
    for (commonPrefix <- commonPrefixes) yield commonPrefix
  }

  private def readLastCBPartitionOffset(cbOffsetKey: String): Option[String] = {
    try {
      val doc = cbBucket.get(cbOffsetKey, classOf[StringDocument])
      Some(doc.content)
    } catch {
      case e @ (_: com.couchbase.client.java.error.DocumentDoesNotExistException | _: java.lang.NullPointerException) => {
        None
      }
    }
  }

  private def saveCBPartitionOffset(cbPartitionOffsetKey: String, cbPartitionOffsetValue: String): Boolean = {
    try {
      cbBucket.upsert(StringDocument.create(cbPartitionOffsetKey, cbPartitionOffsetValue))
      true
    } catch {
      case e: RuntimeException => false
    }
  }

  private def getPartitionOffset(partitionId: Int): Option[String] = {
    val cbPartitionOffsetKey = getCBPartitionOffsetKey(partitionId)
    offsetMap.get(cbPartitionOffsetKey) match {
      case Some(offset) => Some(offset)
      case None => {
        val lastPartitionOffset = readLastCBPartitionOffset(cbPartitionOffsetKey)
        logger.debug(s"readLastCBOffset ==> $lastPartitionOffset")
        lastPartitionOffset match {
          case None => None
          case Some(lastS3Key) => {
            offsetMap.put(cbPartitionOffsetKey, lastS3Key)
            Some(lastS3Key)
          }
        }
      }
    }
  }

  private def getS3KeyObject(s3ObjectKey: String): S3KeyObject = {
    // s3ObjectKey/partitionOffset: ssp-edge-events.2016.03.04.05.29.0.768.3
    logger.debug(s"s3ObjectKey = $s3ObjectKey")
    val s3FileName = s3ObjectKey.split(s3Delimiter).last
    logger.debug(s"s3FileName = $s3FileName")
    val items = s3FileName.split("\\.")
    S3KeyObject(items(0), (items(1)+items(2)+items(3)+items(4)+items(5)).toLong, items(6).toInt, items(7).toLong, items(8).toInt)
  }

  private def sortByOffset(current: ObjectListing): List[S3ObjectSummary] = {
    import scala.collection.JavaConversions._
    current.getObjectSummaries.sortBy(
      objectSummary => getS3KeyObject(objectSummary.getKey).offset
    ).toList
  }

  private def handlePartition(pathToPartition: String, partitionOffset: Option[String]): Int = {
    val current: ObjectListing = s3.listObjects(new ListObjectsRequest().withBucketName(archiveS3BucketName).withPrefix(pathToPartition))
    val s3ObjectSummaryList: List[S3ObjectSummary] = sortByOffset(current)
    val filteredS3ObjectSummaryList: List[S3ObjectSummary] = partitionOffset match {
      case Some(po) => s3ObjectSummaryList.dropWhile(_.getKey != partitionOffset) match {
        case Nil => List.empty[S3ObjectSummary]
        case x :: xs => xs
      }
      case None => s3ObjectSummaryList
    }
    recurHandle(current, filteredS3ObjectSummaryList, getPartitionId(pathToPartition))
  }

  private def getCBPartitionOffsetKey(partitionId: Int): String = cbBucketKeyPrefix+startFrom+"_"+endWith+"_"+partitionId

  private def getPartitionId(pathToPartition: String): Int = pathToPartition.split(s3Delimiter).last.toInt

  private def getPrefix(timeInMinute: Long): String = "topics" + s3Delimiter + archiveKafkaTopic + s3Delimiter + archivePeriod + s3Delimiter + Utils.convertTimeString(timeInMinute.toString)

  private def getLastTimeInMinute(partitionOffset: String): Long = getS3KeyObject(partitionOffset).timeInMinute

}
