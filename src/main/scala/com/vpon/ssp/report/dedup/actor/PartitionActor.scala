package com.vpon.ssp.report.dedup.actor

import scala.concurrent.duration._

import akka.actor.SupervisorStrategy
import akka.actor._
import akka.util.Timeout

import kafka.message.MessageAndMetadata
import kafka.serializer.DefaultDecoder
import com.vpon.ssp.report.dedup.actor.PartitionActorProtocol._
import com.vpon.ssp.report.dedup.actor.PartitionMetricsProtocol._
import com.vpon.ssp.report.dedup.config.DedupConfig
import com.vpon.trade.Event

/**
1. read msg from kafka
2. Read the last sent message from kafka. check if the msg is sent before.
3. Save the key of last message from kafka into couchbase if not found.
4. Check couchbase if it has been processed before,
5. If not, send to new kafka topic
6. once ack received then write to couchbase the key of message is sent and offset
Warning: Not consider case of processed messages with multiple batches but has no offset/dedup key in couchbase (maybe removed or lost)
  */

object PartitionActorProtocol {
  case class JobRequest(mmds: List[MessageAndMetadata[String, Array[Byte]]])
  case object ResumeWork
  case object ResetWork
  case object PauseWork
  case object NextJob
  case class Fulfill(currentSourceEpic: Option[CustomizedIterator[Array[Byte], DefaultDecoder]])
}

object PartitionActor {
  def props(partitionId: Int, master: ActorRef): Props = Props(new PartitionActor(partitionId, master))
}

class PartitionActor(val partitionId: Int, val master:ActorRef) extends Actor with ActorLogging with DedupConfig {
  import context._

  watch(master)

  private val maxRetries = 10
  private val timeRange = 1.minute

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = maxRetries, withinTimeRange = timeRange) {
      case _: ArithmeticException      => SupervisorStrategy.Resume
      case _: NullPointerException     => SupervisorStrategy.Restart
      case _: IllegalArgumentException => SupervisorStrategy.Stop
      case _: Exception                => SupervisorStrategy.Escalate
    }

  implicit val timeout = Timeout(10.second)
  private var isWorkable = true

  private val metrics = context.actorOf(PartitionMetrics.props(partitionId), s"dedup-metrics-${partitionId}")
  private val deduper = new Deduper(system, self, metrics, partitionId)

  private var currentSourceEpic: Option[CustomizedIterator[Array[Byte], DefaultDecoder]] = None

  override def preStart: Unit = {
    log.debug(s"${self.path} ==> PartitionActor preStart")
    super.preStart
    deduper.initOffset
  }

  def receive: akka.actor.Actor.Receive = {
    case jr@JobRequest(mmds) => handleJobRequest(mmds)

    case NextJob => fetchNextJob

    case Fulfill(epic) => {
      currentSourceEpic = epic
      self ! NextJob
    }

    case PauseWork => {
      isWorkable = false
      metrics ! IsWorking(isWorkable)
    }
    case ResumeWork => {
      isWorkable = true
      deduper.initOffset
      metrics ! Resume
    }
    case ResetWork => {
      isWorkable = true
      deduper.initOffset
      metrics ! Reset
    }
    case Terminated(partitionMaster) =>
      log.warning(s"${self.path} ==> PartitionMaster $partitionMaster got terminated!!")
  }

  private def handleJobRequest(mmds: List[MessageAndMetadata[String, Array[Byte]]]) = {
    log.debug(s"${self.path} ==> [STEP 1.1] start handleJobRequest")
    mmds.isEmpty match {
      case true => {
        log.debug(s"${self.path} ==> [STEP 1.2] received 0 batch message")
        scheduleNextJob
      }
      case false => {
        log.debug(s"${self.path} ==> [STEP 1.2] received ${mmds.size} batch messages")
        if (log.isDebugEnabled) {
          for (mmd <- mmds) {
            val key = mmd.key()
            val message = mmd.message()
            try {
              val parsedEvent = Event.parseFrom(message)
              log.debug(s"${self.path} ==> [STEP 1.3] received key: $key . Can parse message into Event: ${parsedEvent}.")
            } catch {
              case e: Exception =>
                log.debug(s"${self.path} ==> [STEP 1.3] received key: $key . Can NOT parse message into Event.")
            }
          }
        }
        log.debug(s"${self.path} ==> [STEP 1.4] end handleJobRequest by doWork")
        deduper.doWork(mmds)
      }
    }
  }

  private def scheduleNextJob() = system.scheduler.scheduleOnce(1.seconds, self, NextJob)

  private def fetchNextJob() = {
    log.debug(s"${self.path} ==> fetchNextJob")
    isWorkable match {
      case true => {
        currentSourceEpic match {
          case None => {
            log.debug(s"${self.path} ==> No more job need to be processed due to no more unprocessed messages in partition-${partitionId} of source topic")
            scheduleNextJob
          }
          case Some(epic) => {
            val iterator = currentSourceEpic.get
            val mmds = iterator.next.toList
            val currentBatchSize = mmds.size
            if (currentBatchSize > 0) log.info(s"${self.path} ==> current batch size: ${currentBatchSize}")
            self ! JobRequest(mmds)
          }
        }
      }
      case false => {
        log.debug(s"${self.path} ==> No more job need to be processed due to PartitionActor-${partitionId} was paused")
        scheduleNextJob
      }
    }
  }
}