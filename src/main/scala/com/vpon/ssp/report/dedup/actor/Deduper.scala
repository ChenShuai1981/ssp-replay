package com.vpon.ssp.report.dedup.actor


import scala.concurrent.Future
import scala.util.{Failure, Success}

import akka.actor.{ActorSystem, ActorRef}
import com.couchbase.client.java.document.StringDocument
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.producer.KeyedMessage
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.commons.lang3.exception.ExceptionUtils

import com.vpon.ssp.report.common.couchbase.{BucketWithKeyPrefix, RxCouchbaseBucket, CBExtension}
import com.vpon.ssp.report.common.kafka.consumer.TopicsConsumer
import com.vpon.ssp.report.common.kafka.consumer.TopicsConsumer.{AbsoluteOffset, EndOffset, RelativeOffset}
import com.vpon.ssp.report.common.kafka.producer.CustomPartitionProducer
import com.vpon.ssp.report.common.model.{EventRecordJsonProtocol, EventRecord}
import com.vpon.ssp.report.common.util.Retry.NeedRetryException
import com.vpon.ssp.report.common.util.Retry
import com.vpon.ssp.report.dedup.actor.PartitionActorProtocol.{Fulfill, NextJob, PauseWork}
import com.vpon.ssp.report.dedup.actor.PartitionMetricsProtocol._
import com.vpon.ssp.report.dedup.config.DedupConfig
import com.vpon.ssp.report.dedup.flatten.exception.{FlattenFailureType, FlattenFailure}
import com.vpon.ssp.report.dedup.flatten.{Flattener, FlattenerTrait}
import com.vpon.trade.Event

case class KafkaException(msg: String) extends Exception(msg)
case class CouchbaseException(msg: String) extends Exception(msg)

class Deduper(private val system: ActorSystem,
              private val partitionActor: ActorRef,
              private val partitionMetrics: ActorRef,
              private val partitionId: Int,
              private val flattener: FlattenerTrait = Flattener.getInstance) extends DedupConfig {

  implicit val dispatcher = system.dispatcher

  private val log = system.log

  private val sourceKafkaParams = Map("metadata.broker.list" -> sourceBrokers)
  private val sourceTopicPartition = TopicAndPartition(sourceTopic, partitionId)

  private val offsetBucketWithKeyPrefix = CBExtension(system).buckets("offset")
  private val dedupBucketWithKeyPrefix = CBExtension(system).buckets("dedup")

  private val offsetBucket          = offsetBucketWithKeyPrefix.bucket
  private val offsetBucketKeyPrefix = offsetBucketWithKeyPrefix.keyPrefix

  private val dedupBucket          = dedupBucketWithKeyPrefix.bucket
  private val dedupBucketKeyPrefix = dedupBucketWithKeyPrefix.keyPrefix

  private val cbOffsetKey          = s"${offsetBucketKeyPrefix}${partitionId}"

  private val targetKafkaParams    = Map("metadata.broker.list" -> flattenEventsBrokers)
  private val targetTopicPartition = TopicAndPartition(flattenEventsTopic, partitionId)
  private val targetTopicsConsumer = TopicsConsumer[String, String, StringDecoder, StringDecoder](
    targetKafkaParams, Set(targetTopicPartition), RelativeOffset(-1), EndOffset(EndOffset.FROM_CURRENT_LATEST))

  private val targetKafkaProducer  = new CustomPartitionProducer[String, String](flattenEventsBrokers)
  private val delayedKafkaProducer = new CustomPartitionProducer[String, String](delayedEventsBrokers)
  private val warningKafkaProducer = new CustomPartitionProducer[String, String](warningEventsBrokers)

  @volatile
  private var sourceTopicsConsumer: Future[TopicsConsumer[String, Array[Byte], StringDecoder, DefaultDecoder, MessageAndMetadata[String, Array[Byte]]]] = _

  def initOffset = {
    val offsetF: Future[Long] = retryCouchbaseOperation{
      log.debug(s"${partitionActor.path} ==> cbOffsetKey: $cbOffsetKey")
      offsetBucket.get[StringDocument](cbOffsetKey)
    }.map(_.content) map { f =>
      log.debug(s"${partitionActor.path} ==> offset: ${f.toLong}")
      f.toLong
    } recover {
      case e @ (_: com.couchbase.client.java.error.DocumentDoesNotExistException | _: java.lang.NullPointerException) => {
        0
      }
    }

    sourceTopicsConsumer = offsetF map { offset =>
      log.debug(s"${partitionActor.path} ==> LastOffset: $offset")
      partitionMetrics ! LastOffset(offset)
      log.debug(s"${partitionActor.path} ==> constructing TopicsConsumer: sourceKafkaParams -> $sourceKafkaParams, sourceTopicPartition -> $sourceTopicPartition, AbsoluteOffset -> $offset")
      TopicsConsumer[String, Array[Byte], StringDecoder, DefaultDecoder](sourceKafkaParams, Set(sourceTopicPartition), AbsoluteOffset(offset))
    }

    sourceTopicsConsumer onComplete {
      case Success(s) => {
        log.debug(s"${partitionActor.path} ==> sourceTopicsConsumer success. consumeBatchSize: $consumeBatchSize")
        partitionActor ! Fulfill(Some(new CustomizedIterator[Array[Byte], DefaultDecoder](consumeBatchSize, s)))
      }
      case Failure(e) =>
        log.debug(s"${partitionActor.path} ==> sourceTopicsConsumer failure. ${ExceptionUtils.getStackTrace(e)}")
        val err = s"${partitionActor.path} ==> Failed to start PartitionActor for partition $partitionId, so pause work!!\n${ExceptionUtils.getStackTrace(e)}"
        log.error(e, err)
        partitionMetrics ! Error(err)
        partitionActor ! PauseWork
    }
  }

  def doWork(mmds: List[MessageAndMetadata[String, Array[Byte]]]): Future[Any] = {
    val f = for {
      dedupedMmds <- dedup(mmds)
      eventRecords <- flatten(dedupedMmds)
      sendResult <- send(eventRecords)
    } yield {
        sendResult match {
          case None => {
            log.debug(s"${partitionActor.path} ==> [STEP 6.6] sent failure. But continue to process next batch messages without blocking")
            doPostSendActions(mmds, List.empty[EventRecord])
          } // all deduped
          case Some(isSentSuccess) => {
            if (isSentSuccess) {
              log.debug(s"${partitionActor.path} ==> [STEP 6.6] sent success. continue to doPostSendActions.")
              doPostSendActions(mmds, eventRecords)
            } else {
              log.warning(s"${partitionActor.path} ==> [STEP 6.6] sent failure?? But this branch should not be go through!!")
              doPostSendActions(mmds, eventRecords)
            }
          }
        }
      }

    f.recover{
      case e @ (_: KafkaException | _: CouchbaseException) => {
        val time = System.currentTimeMillis()
        log.error(e, e.getMessage)
        partitionMetrics ! Error(e.getMessage)
        log.error(s"${partitionActor.path} ==> [STEP 6.6] Caught fatal exception ${e.getClass} when doWork at $time, so pause work!\n${ExceptionUtils.getStackTrace(e)}")
        partitionActor ! PauseWork
      }
    }
  }

  private def dedup(mmds: List[MessageAndMetadata[String, Array[Byte]]]): Future[List[MessageAndMetadata[String, Array[Byte]]]] = {
    val dedupStartTime = System.currentTimeMillis()
    mmds.isEmpty match {
      case true => Future {
        mmds
      }
      case false =>
        for {
          selfDedupedMmds <- selfDedup(mmds)
          selfDedupEndTime = System.currentTimeMillis()
          _ = partitionMetrics ! SelfDedup(mmds.size, selfDedupedMmds.size, selfDedupEndTime - dedupStartTime)

          couchbaseDedupedMmds <- couchbaseDedup(selfDedupedMmds)
          couchbaseDedupEndTime = System.currentTimeMillis()
          _ = partitionMetrics ! CouchbaseDedup(selfDedupedMmds.size, couchbaseDedupedMmds.size, couchbaseDedupEndTime - selfDedupEndTime)

          kafkaDedupedMmds <- kafkaDedup(couchbaseDedupedMmds)
          kafkaDedupEndTime = System.currentTimeMillis()
          _ = partitionMetrics ! KafkaDedup(couchbaseDedupedMmds.size, kafkaDedupedMmds.size, kafkaDedupEndTime - couchbaseDedupEndTime)
        } yield {
          kafkaDedupedMmds
        }
    }
  }

  private def selfDedup(mmds: List[MessageAndMetadata[String, Array[Byte]]]): Future[List[MessageAndMetadata[String, Array[Byte]]]] = {
    log.debug(s"${partitionActor.path} ==> [STEP 2.1] start selfDedup. In size: ${mmds.size}")
    val map = mmds.map(mmd => (mmd.key() -> mmd)).toMap
    if (log.isDebugEnabled) {
      val keys = map.keys.toList
      log.debug(s"${partitionActor.path} ==> [STEP 2.2] end selfDedup. Out size: ${keys.size}, keys -> $keys")
    }
    Future{ map.values.toList }
  }

  private def couchbaseDedup(selfDedupedMmds: List[MessageAndMetadata[String, Array[Byte]]]): Future[List[MessageAndMetadata[String, Array[Byte]]]] = {
    log.debug(s"${partitionActor.path} ==> [STEP 3.1] start couchbaseDedup. In size: ${selfDedupedMmds.size}")
    val selfDedupedKeys = selfDedupedMmds.map(mmd => s"$dedupBucketKeyPrefix${mmd.key()}").toArray
    log.debug(s"${partitionActor.path} ==> [STEP 3.2] selfDedupedKeys: ${selfDedupedKeys.mkString(",")}")
    retryCouchbaseOperation {
      dedupBucket.getBulk[StringDocument](selfDedupedKeys).map(docs => {
        val couchbaseExistedKeys = docs.map(doc => doc.id())
        log.debug(s"${partitionActor.path} ==> [STEP 3.3] couchbaseExistedKeys: ${couchbaseExistedKeys.mkString(",")}")
        val result = selfDedupedMmds.filterNot(mmd => couchbaseExistedKeys.contains(s"$dedupBucketKeyPrefix${mmd.key()}"))
        if (log.isDebugEnabled) {
          val keys = result.map(_.key())
          log.debug(s"${partitionActor.path} ==> [STEP 3.4] end couchbaseDedup. Out size: ${result.size}, keys -> $keys")
        }
        result
      })
    }
  }

  private def kafkaDedup(couchbaseDedupedMmds: List[MessageAndMetadata[String, Array[Byte]]]): Future[List[MessageAndMetadata[String, Array[Byte]]]] = {
    log.debug(s"${partitionActor.path} ==> [STEP 4.1] start kafkaDedup. In size: ${couchbaseDedupedMmds.size}")
    couchbaseDedupedMmds.isEmpty match {
      case true => {
        log.debug(s"${partitionActor.path} ==> [STEP 4.2] since couchbase deduped messages is empty, so there is no message returned after kafkaDedup.")
        Future{List.empty}
      }
      case false => {
        // consume last message of target sent topic partition
        // lastSentMsgKey = $lastEventKey_$partitionId (fix message seek origin partition issue, see CustomPartitionProducer)
        val lastSentMsgKey: Option[String] = {
          try {
            if (targetTopicsConsumer.hasNext) {
              Some(targetTopicsConsumer.next.key)
            } else {
              None
            }
          } catch {
            case e:Exception => {
              throw KafkaException(s"${partitionActor.path} ==> [STEP 4.2] failed to get last sent message key of partition $partitionId from kafka topic 2")
            }
          }
        }
        lastSentMsgKey match {
          case None => {
            log.debug(s"${partitionActor.path} ==> [STEP 4.3] not found last sent message key")
            Future{ couchbaseDedupedMmds }
          }
          case Some(k) => {
            log.debug(s"${partitionActor.path} ==> [STEP 4.3] found last sent message key is $k")
            /**
             * val list = Seq(1,2,3,4,5,6)
             * list.span(_ != 4)
             * (List(1, 2, 3),List(4, 5, 6))
             * list.span(_ != 7)
             * (List(1, 2, 3, 4, 5, 6),List())
             *
             * but what we expected it (List(1, 2, 3, 4),List(5, 6))
             * so we have to move 'head' of part2 into part1 if part2 not empty
             * if part2 empty all part1 should be sent
             */
            val keyParts = couchbaseDedupedMmds span (mmd => s"${mmd.key}_${partitionId}" != k)
            log.debug(s"${partitionActor.path} ==> [STEP 4.4] couchbase deduped keys: ${couchbaseDedupedMmds.map(_.key)}")
            val keyPart1 = keyParts._1
            val keyPart2 = keyParts._2
            val (alreadySentList, toBeSentList) = if (!keyPart2.isEmpty) (keyPart1 :+ keyPart2.head, keyPart2.tail) else (keyPart2, keyPart1)
            val alreadySentKeys: List[String] = alreadySentList.map(_.key)
            log.debug(s"${partitionActor.path} ==> [STEP 4.5] alreadySentKeys: ${alreadySentKeys.mkString(",")}, toBeSentKeys: ${toBeSentList.map(_.key).mkString(",")}")

            def getF : Future[Seq[StringDocument]]= {
              val alreadySentKeysArray = alreadySentKeys.toArray
              retryCouchbaseOperation {
                dedupBucket.getBulk[StringDocument](alreadySentKeysArray)
              }
            }
            def upsertF(missedDocs: List[StringDocument]): Future[Seq[StringDocument]] = {
              retryCouchbaseOperation {
                dedupBucket.upsertBulk[StringDocument](missedDocs)
              }
            }

            val f = for {
              couchbaseExistedDocs <- getF
              couchbaseExistedKeys = couchbaseExistedDocs.map(_.id())
              _ = log.debug(s"${partitionActor.path} ==> [STEP 4.6] couchbaseExistedKeys: ${couchbaseExistedKeys.mkString(",")}")
              missedSaveKeys = alreadySentKeys diff couchbaseExistedKeys
              _ = log.debug(s"${partitionActor.path} ==> [STEP 4.7] missedSaveKeys: ${missedSaveKeys.mkString(",")}")
              missedDocs = missedSaveKeys.map(key => StringDocument.create(s"$dedupBucketKeyPrefix$key", dedupKeyTTL.toSeconds.toInt, ""))
              upsertedDocs <- upsertF(missedDocs)
              _ = log.debug(s"${partitionActor.path} ==> [STEP 4.8] success to upsert missedSaveKeys: ${missedSaveKeys.mkString(",")}")
            } yield {
                toBeSentList
              }

            f.recover{
              case e: Throwable => {
                val err = s"${partitionActor.path} ==> [STEP 4.8] failed to get alreadySentKeys and/or upsert missedSaveKeys.\n${ExceptionUtils.getStackTrace(e)}"
                throw CouchbaseException(err)
              }
            }
          }
        }
      }
    }
  }

  private def flatten(dedupedMmds: List[MessageAndMetadata[String, Array[Byte]]]): Future[List[EventRecord]] = {
    log.debug(s"${partitionActor.path} ==> [STEP 5.1] start flatten")
    if (!dedupedMmds.isEmpty) {
      Future.traverse(dedupedMmds){m => flattenMessage(m)} map {
        k => {
          val t = k.flatten
          log.debug(s"${partitionActor.path} ==> [STEP 5.3] end flatten with result: ${t}")
          t
        }
      }
    } else {
      log.debug(s"${partitionActor.path} ==> [STEP 5.3] end flatten with empty result")
      Future{ List.empty }
    }
  }

  private def flattenMessage(mmd: MessageAndMetadata[String, Array[Byte]]): Future[Option[EventRecord]] = {
    val offset = mmd.offset
    val key = mmd.key()
    partitionMetrics ! Consume(offset, key)
    val flattenStartTime = System.currentTimeMillis()
    flattener.convert(mmd.message) map { result =>
      log.debug(s"Flatten result: $result")
      result match {
        case Right(r) => {
          log.debug(s"${partitionActor.path} ==> [STEP 5.2] flatten success. $r")
          partitionMetrics ! Flatten(System.currentTimeMillis - flattenStartTime)
          Some(r)
        }
        case Left(f: FlattenFailure) => f.errorType match {
          case FlattenFailureType.DelayedEvent => {
            partitionMetrics ! DelayedEvent(offset, key)
            handleDelayedFlattenFailureEvents(f, mmd)
          }
          case FlattenFailureType.MappingError => {
            partitionMetrics ! MappingError(offset, key)
            handleWarningFlattenFailureEvents(f, mmd)
          }
          case FlattenFailureType.PlacementNotFound => {
            partitionMetrics ! PlacementNotFound(offset, key)
            handleWarningFlattenFailureEvents(f, mmd)
          }
          case FlattenFailureType.PublisherNotFound => {
            partitionMetrics ! PublisherNotFound(offset, key)
            handleWarningFlattenFailureEvents(f, mmd)
          }
          case FlattenFailureType.ExchangeRateNotFound => {
            partitionMetrics ! ExchangeRateNotFound(offset, key)
            handleWarningFlattenFailureEvents(f, mmd)
          }
          case FlattenFailureType.PublisherSspTaxRateNotFound => {
            partitionMetrics ! PublisherSspTaxRateNotFound(offset, key)
            handleWarningFlattenFailureEvents(f, mmd)
          }
          case FlattenFailureType.DspSspTaxRateNotFound => {
            partitionMetrics ! DspSspTaxRateNotFound(offset, key)
            handleWarningFlattenFailureEvents(f, mmd)
          }
          case FlattenFailureType.CouchbaseDeserializationError => {
            partitionMetrics ! CouchbaseDeserializationError(offset, key)
            handleWarningFlattenFailureEvents(f, mmd)
          }
          case FlattenFailureType.InvalidEvent => {
            partitionMetrics ! InvalidEvent(offset, key)
            handleWarningFlattenFailureEvents(f, mmd)
          }
          case FlattenFailureType.UnParsedEvent => {
            partitionMetrics ! UnParsedEvent(offset, key)
            handleWarningFlattenFailureEvents(f, mmd)
          }
          case FlattenFailureType.UnknownEventType => {
            partitionMetrics ! UnknownEventType(offset, key)
            handleWarningFlattenFailureEvents(f, mmd)
          }
          case FlattenFailureType.SecretKeyNotFound => {
            partitionMetrics ! SecretKeyNotFound(offset, key)
            handleWarningFlattenFailureEvents(f, mmd)
          }
          case FlattenFailureType.DecryptClearPriceError => {
            partitionMetrics ! DecryptClearPriceError(offset, key)
            handleWarningFlattenFailureEvents(f, mmd)
          }
          case FlattenFailureType.UnsupportedPublisherRevenueShareType => {
            partitionMetrics ! UnsupportedPublisherRevenueShareType(offset, key)
            handleWarningFlattenFailureEvents(f, mmd)
          }
          case FlattenFailureType.UnsupportedSellerRevenueShareType => {
            partitionMetrics ! UnsupportedSellerRevenueShareType(offset, key)
            handleWarningFlattenFailureEvents(f, mmd)
          }
          case FlattenFailureType.UnsupportedDealType => {
            partitionMetrics ! UnsupportedDealType(offset, key)
            handleWarningFlattenFailureEvents(f, mmd)
          }
          case FlattenFailureType.CouchbaseError => {
            partitionMetrics ! CouchbaseError(offset, key)
            handleErrorFlattenFailureEvents(f, mmd)
          }
          case FlattenFailureType.UnknownError => {
            partitionMetrics ! UnknownError(offset, key)
            handleErrorFlattenFailureEvents(f, mmd)
          }
        }
      }
    }
  }

  private def handleDelayedFlattenFailureEvents(f: FlattenFailure, mmd: MessageAndMetadata[String, Array[Byte]]) = {
    partitionMetrics ! Warning(f.message)
    log.warning(s"${partitionActor.path} ==> [STEP 5.2] flatten failure due to ${f.errorType}. key: ${mmd.key}, offset: ${mmd.offset}, send it to delayed topic and continue.")
    sendDelayedMessage(mmd)
    None
  }

  private def handleWarningFlattenFailureEvents(f: FlattenFailure, mmd: MessageAndMetadata[String, Array[Byte]]) = {
    partitionMetrics ! Warning(f.message)
    log.warning(s"${partitionActor.path} ==> [STEP 5.2] flatten warning failure due to ${f.errorType}. key: ${mmd.key}, offset: ${mmd.offset}, send it to warning topic and continue.")
    sendWarningMessage(mmd)
    None
  }

  private def handleErrorFlattenFailureEvents(f: FlattenFailure, mmd: MessageAndMetadata[String, Array[Byte]]) = {
    partitionMetrics ! Error(f.message)
    log.error(s"${partitionActor.path} ==> [STEP 5.2] flatten error failure due to ${f.errorType}. key: ${mmd.key}, offset: ${mmd.offset}, so pause work!!")
    partitionActor ! PauseWork
    None
  }

  private def sendDelayedMessage(mmd: MessageAndMetadata[String, Array[Byte]]): Unit = {
    val delayedMessage = new KeyedMessage(delayedEventsTopic, mmd.key(), Event.parseFrom(mmd.message).toJson())
    log.debug(s"to send delay message: $delayedMessage")
    delayedKafkaProducer.sendMessages(Seq(delayedMessage))
    log.debug(s"sent delay message done")
  }

  private def sendWarningMessage(mmd: MessageAndMetadata[String, Array[Byte]]): Unit = {
    val warningMessage = new KeyedMessage(warningEventsTopic, mmd.key(), Event.parseFrom(mmd.message).toJson())
    log.debug(s"to send warning message: $warningMessage")
    warningKafkaProducer.sendMessages(Seq(warningMessage))
    log.debug(s"sent warning message done")
  }

  private def getDedupMessageKey(edgeMessageKey: String, partitionId: Int) = s"${edgeMessageKey}_${partitionId}"

  private def send(eventRecords: List[EventRecord]): Future[Option[Boolean]] = {
    log.debug(s"${partitionActor.path} ==> [STEP 6.1] start send")
    if (!eventRecords.isEmpty) {
      import EventRecordJsonProtocol._
      import spray.json._
      val keyedMessages = eventRecords.map(eventRecord => {
        val dedupMessageKey = getDedupMessageKey(eventRecord.event_key, partitionId)
        new KeyedMessage(flattenEventsTopic, dedupMessageKey, eventRecord.toJson.compactPrint)
      })
      if (log.isDebugEnabled) {
        for (keyedMessage <- keyedMessages) {
          log.debug(s"${partitionActor.path} ==> [STEP 6.2] constructed messages: [key] -> ${keyedMessage.key}, [message] -> ${keyedMessage.message}")
        }
      }
      val sendStartTime = System.currentTimeMillis()
      try {
        targetKafkaProducer.sendMessages(keyedMessages)
        log.info(s"${partitionActor.path} ==> [STEP 6.3] sent success ${keyedMessages.size} messages to topic 2")
      } catch {
        case e: Throwable => {
          val err = s"${partitionActor.path} ==> [STEP 6.3] sent failure list of EventRecords to kafka topic 2, so pause work!! EventRecords -> ${eventRecords}\n${ExceptionUtils.getStackTrace(e)}"
          throw KafkaException(err)
        }
      }
      if (log.isDebugEnabled) {
        for (keyedMessage <- keyedMessages) {
          val key = keyedMessage.key
          val message = keyedMessage.message
          log.debug(s"${partitionActor.path} ==> [STEP 6.4] already sent message: [key] -> $key, [message] -> $message")
        }
      }
      val sendUsedTime = System.currentTimeMillis() - sendStartTime
      partitionMetrics ! Send(keyedMessages.size, sendUsedTime)
      log.debug(s"${partitionActor.path} ==> [STEP 6.5] end send with true result.")
      Future{Some(true)}
    } else {
      log.debug(s"${partitionActor.path} ==> [STEP 6.5] end send with None result.")
      Future{None}
    }
  }

  private def doPostSendActions(mmds: List[MessageAndMetadata[String, Array[Byte]]], eventRecords: List[EventRecord]) = {
    val f = for {
      addDedupKeyResult      <- addDedupKeys(eventRecords)
      updateLastOffsetResult <- updateLastOffset(mmds) if (addDedupKeyResult.isDefined && addDedupKeyResult.get)
    } yield {
        partitionActor ! NextJob
      }

    f.recover{
      case e:CouchbaseException => {
        val time = System.currentTimeMillis()
        log.error(e, e.getMessage)
        partitionMetrics ! Error(e.getMessage)
        log.error(s"${partitionActor.path} ==> Caught CouchbaseException when doPostSendActions at $time, so pause work!!.\n${ExceptionUtils.getStackTrace(e)}")
        partitionActor ! PauseWork
      }
      case e:Throwable => {
        val time = System.currentTimeMillis()
        log.warning(s"${partitionActor.path} ==> Caught Throwable when doPostSendActions at $time, but still continue fetch next job.")
        partitionActor ! NextJob
      }
    }
  }

  private def addDedupKeys(eventRecords: List[EventRecord]): Future[Option[Boolean]] = {
    log.debug(s"${partitionActor.path} ==> [STEP 7.1] start addDedupKeys")
    if (!eventRecords.isEmpty) {
      val eventKeys = eventRecords.map(_.event_key)
      log.debug(s"${partitionActor.path} ==> [STEP 7.2] eventKeys: $eventKeys")
      val dedupDocs = eventKeys.map(key => StringDocument.create(s"$dedupBucketKeyPrefix$key", dedupKeyTTL.toSeconds.toInt, ""))
      retryCouchbaseOperation{
        dedupBucket.upsertBulk[StringDocument](dedupDocs).map(_ => {
          log.debug(s"${partitionActor.path} ==> [STEP 7.3] Success to add dedup keys: $eventKeys")
          log.debug(s"${partitionActor.path} ==> [STEP 7.4] end addDedupKeys with true result.")
          Some(true)
        }) recover {
          case e: Throwable => {
            log.error(s"${partitionActor.path} ==> [STEP 7.3] Failed to add dedup keys: $eventKeys")
            throw CouchbaseException(e.getMessage)
          }
        }
      }
    } else {
      log.debug(s"${partitionActor.path} ==> [STEP 7.4] end addDedupKeys with None result.")
      Future{None}
    }
  }

  private def updateLastOffset(mmds: List[MessageAndMetadata[String, Array[Byte]]]): Future[Option[Boolean]] = {
    log.debug(s"${partitionActor.path} ==> [STEP 8.1] start updateLastOffset")
    if (!mmds.isEmpty) {
      val lastOffset = mmds.last.offset
      val toSaveOffset = lastOffset + 1
      log.debug(s"${partitionActor.path} ==> [STEP 8.2] toSaveOffset: $toSaveOffset")
      retryCouchbaseOperation{
        offsetBucket.upsert(StringDocument.create(cbOffsetKey, toSaveOffset.toString))
      } map {
        strDoc => {
          log.info(s"${partitionActor.path} ==> [STEP 8.3] Success to update last offset: $cbOffsetKey -> $toSaveOffset.")
          partitionMetrics ! LastOffset(toSaveOffset)
          log.debug(s"${partitionActor.path} ==> [STEP 8.4] end updateLastOffset with true result.")
          Some(true)
        }
      } recover {
        case e: Throwable => {
          val err = s"${partitionActor.path} ==> [STEP 8.3] Failed to update last offset: $cbOffsetKey -> $toSaveOffset\n${ExceptionUtils.getStackTrace(e)}"
          throw CouchbaseException(err)
        }
      }
    } else {
      log.debug(s"${partitionActor.path} ==> [STEP 8.2] end updateLastOffset with None result.")
      Future{None}
    }
  }

  def retryCouchbaseOperation[T](task: Future[T]): Future[T] = {
    val retryTask = Retry(system.scheduler, couchbaseMaxRetries, couchbaseRetryInterval) {
      task.recover {
        case e: com.couchbase.client.core.RequestCancelledException =>
          val msg = s"com.couchbase.client.core.RequestCancelledException ==> Try again after ${couchbaseRetryInterval}ms. ${ExceptionUtils.getStackTrace(e)}"
          log.warning(msg)
          throw new NeedRetryException(msg)
        case e: com.couchbase.client.java.error.TemporaryFailureException =>
          val msg = s"com.couchbase.client.java.error.TemporaryFailureException ==> Try again after ${couchbaseRetryInterval}ms. ${ExceptionUtils.getStackTrace(e)}"
          log.warning(msg)
          throw new NeedRetryException(msg)
        case e: java.net.ConnectException =>
          val msg = s"java.net.ConnectException ==> Try again after ${couchbaseRetryInterval}ms. ${ExceptionUtils.getStackTrace(e)}"
          log.warning(msg)
          throw new NeedRetryException(msg)
        case e: RuntimeException if (e.getMessage.equals("java.util.concurrent.TimeoutException")) =>
          val msg = s"RuntimeException (java.util.concurrent.TimeoutException) ==> Try again after ${couchbaseRetryInterval}ms. ${ExceptionUtils.getStackTrace(e)}"
          log.warning(msg)
          throw new NeedRetryException(msg)
      }
    }

    retryTask.recover {
      case e: NeedRetryException => {
        log.error(s"Failed to retryCouchbaseAction.")
        throw new CouchbaseException(e.getMessage)
      }
    }
  }
}