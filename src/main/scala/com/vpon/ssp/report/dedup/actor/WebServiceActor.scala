package com.vpon.ssp.report.dedup.actor

import java.lang.management.ManagementFactory
import javax.management.ObjectName

import scala.collection.immutable.ListMap
import scala.concurrent.Future
import scala.io.Source
import scala.util.{Failure, Success}

import akka.actor._
import akka.pattern.{AskTimeoutException, ask}
import akka.util.Timeout
import spray.json._

import com.couchbase.client.java.document.StringDocument
import com.google.common.cache.CacheStats
import kafka.producer.KeyedMessage
import org.apache.commons.lang3.exception.ExceptionUtils

import WebServiceActor._
import spray.can.Http
import spray.can.Http.Unbind
import spray.http.HttpMethods._
import spray.http.MediaTypes._
import spray.http._
import spray.httpx.unmarshalling._
import spray.httpx.SprayJsonSupport._
import com.vpon.ssp.report.common.couchbase.CBExtension
import com.vpon.ssp.report.dedup.flatten.Flattener
import com.vpon.ssp.report.dedup.flatten.exception.FlattenFailure
import com.vpon.ssp.report.edge.Trade.TradeLogJsonProtocol
import com.vpon.ssp.report.common.model.{EventRecord, EventRecordJsonProtocol}
import EventRecordJsonProtocol._
import TradeLogJsonProtocol._
import scala.concurrent.duration._
import scala.util.Try

import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write

import com.vpon.mapping.{DeviceTypeMapping, GeographyMapping}
import com.vpon.ssp.report.common.kafka.producer.CustomPartitionProducer
import com.vpon.ssp.report.dedup.actor.PartitionActorProtocol.{ResumeWork, PauseWork, ResetWork}
import com.vpon.ssp.report.dedup.actor.PartitionMasterProtocol.{PartitionStat, GetKafkaConnection, ReportPartitionStat}
import com.vpon.ssp.report.dedup.actor.PartitionMetricsProtocol.{GetInfo, GetMetrics}
import com.vpon.ssp.report.dedup.config.DedupConfig
import com.vpon.ssp.report.edge.Trade.{EdgeConvertFailure, EdgeEvent}
import com.vpon.ssp.report.dedup.flatten._
import com.vpon.ssp.report.dedup.flatten.exception.FlattenFailure
import com.vpon.ssp.report.common.model.EventRecord


object WebServiceActor {
  case object Shutdown
  case object ShutdownCompleted

  sealed trait Command
  case object Reset extends Command
  case object Resume extends Command
  case object Pause extends Command

  def jvmMBeansStatsPresentation(): JsObject = {
    val osMXBean = ManagementFactory.getOperatingSystemMXBean
    val threadMXBean = ManagementFactory.getThreadMXBean
    val memoryMXBean = ManagementFactory.getMemoryMXBean

    JsObject("HeapMemoryUsage_init" -> JsNumber(memoryMXBean.getHeapMemoryUsage.getInit.toString),
      "HeapMemoryUsage_used" -> JsNumber(memoryMXBean.getHeapMemoryUsage.getUsed.toString),
      "NonHeapMemoryUsage_init" -> JsNumber(memoryMXBean.getNonHeapMemoryUsage.getInit.toString),
      "NonHeapMemoryUsage_used" -> JsNumber(memoryMXBean.getNonHeapMemoryUsage.getUsed.toString),
      "SystemLoadAverage" -> JsNumber(osMXBean.getSystemLoadAverage.toString),
      "PeakThreadCount" -> JsNumber(threadMXBean.getPeakThreadCount.toString),
      "ThreadCount" -> JsNumber(threadMXBean.getThreadCount.toString)
    )
  }

  def akkaMBeansStatsPresentation(): JsObject = {
    val mbeanServer = ManagementFactory.getPlatformMBeanServer
    val clusterObjName = new ObjectName("akka:type=Cluster")
    JsObject("Available" -> JsString(mbeanServer.getAttribute(clusterObjName, "Available").toString),
      "ClusterStatus" -> JsonParser(mbeanServer.getAttribute(clusterObjName, "ClusterStatus").toString),
      "Leader" -> JsString(mbeanServer.getAttribute(clusterObjName, "Leader").toString),
      "MemberStatus" -> JsString(mbeanServer.getAttribute(clusterObjName, "MemberStatus").toString),
      "Members" -> JsString(mbeanServer.getAttribute(clusterObjName, "Members").toString),
      "Unreachable" -> JsString(mbeanServer.getAttribute(clusterObjName, "Unreachable").toString)
    )
  }

  def allCacheStatsPresentation: JsObject =
    JsObject(
      "DeviceType" -> cacheStatsPresentation(DeviceTypeMapping.cacheStats),
      "Geography" -> cacheStatsPresentation(GeographyMapping.cacheStats)
    )

  def cacheStatsPresentation(stats: CacheStats): JsObject = {
    JsObject(
      "HitCount" -> JsNumber(stats.hitCount),
      "HitRate" -> JsNumber(stats.hitRate),
      "LoadCount" -> JsNumber(stats.loadCount),
      "LoadExceptionCount" -> JsNumber(stats.loadExceptionCount),
      "LoadExceptionRate" -> JsNumber(stats.loadExceptionRate),
      "LoadSuccessCount" -> JsNumber(stats.loadSuccessCount),
      "AverageLoadPenalty" -> JsNumber(stats.averageLoadPenalty),
      "EvictionCount" -> JsNumber(stats.evictionCount),
      "MissCount" -> JsNumber(stats.missCount),
      "MissRate" -> JsNumber(stats.missRate),
      "TotalLoadTime" -> JsNumber(stats.totalLoadTime)
    )
  }

  def mergeMap[A, B](ms: Iterable[Map[A, B]])(f: (B, B) => B): Map[A, B] = {
    (Map[A, B]() /: (for (m <- ms; kv <- m) yield kv)) { (a, kv) =>
      a + (if (a.contains(kv._1)) kv._1 -> f(a(kv._1), kv._2) else kv)
    }
  }

  def props(): Props = Props(new WebServiceActor())
}

class WebServiceActor extends Actor with ActorLogging with DedupConfig {
  private[this] implicit val format = Serialization.formats(NoTypeHints).withBigDecimal
  private[this] implicit val dispatcher = context.dispatcher
  private[this] implicit val timeout: Timeout = 10.second
  private[this] val classLoader = getClass().getClassLoader()
  private[this] var httpListener: ActorRef = _

  def receive: akka.actor.Actor.Receive = {
    case _: Http.Connected => sender ! Http.Register(self)
    case _: Http.Bound => httpListener = sender()
    case Shutdown => shutdown()
    case HttpRequest(GET, Uri.Path("/"), _, _, _) => index()
    case HttpRequest(GET, Uri.Path("/buildinfo"), _, _, _) => buildInfo()
    case HttpRequest(GET, Uri.Path("/healthchk"), _, _, _) => healthchk()
    case HttpRequest(POST, uri@Uri.Path("/flatten"), headers, entity, _) => flatten(uri, headers, entity)
    case HttpRequest(POST, uri@Uri.Path("/send"), headers, entity, _) => send(uri, headers, entity)
    case HttpRequest(GET, uri@Uri.Path("/jvm"), _, _, _) => {
      sender ! HttpResponse(entity = HttpEntity(`application/json`, jvmMBeansStatsPresentation.compactPrint))
    }
    case HttpRequest(GET, uri@Uri.Path("/akka"), _, _, _) => {
      sender ! HttpResponse(entity = HttpEntity(`application/json`, akkaMBeansStatsPresentation.compactPrint))
    }
    case HttpRequest(GET, uri@Uri.Path("/cache"), _, _, _) => {
      sender ! HttpResponse(entity = HttpEntity(`application/json`, allCacheStatsPresentation.compactPrint))
    }
    case HttpRequest(GET, Uri.Path("/partitionStat"), _, _, _) => reportPartitionStat()
    case HttpRequest(GET, Uri.Path("/metrics"), _, _, _) => reportMetrics()
    case HttpRequest(GET, uri@Uri.Path("/lastOffset"), _, _, _) => updateLastOffset(uri)
    case HttpRequest(GET, uri@Uri.Path("/info"), _, _, _) => info(uri)
    case HttpRequest(GET, Uri.Path("/kafka"), _, _, _) => reportKafka()
    case HttpRequest(GET, Uri.Path("/config"), _, _, _) => showConfig()
    case HttpRequest(GET, Uri.Path("/pauseAll"), _, _, _) => operateAll(Pause)
    case HttpRequest(GET, Uri.Path("/resumeAll"), _, _, _) => operateAll(Resume)
    case HttpRequest(GET, Uri.Path("/resetAll"), _, _, _) => operateAll(Reset)
    case HttpRequest(GET, uri@Uri.Path("/pause"), _, _, _) => operate(uri, Pause)
    case HttpRequest(GET, uri@Uri.Path("/resume"), _, _, _) => operate(uri, Resume)
    case HttpRequest(GET, uri@Uri.Path("/reset"), _, _, _) => operate(uri, Reset)
    case HttpRequest(GET, uri@Uri.Path("/loglevel"), _, _, _) => updateLogLevel(uri)
    case _: HttpRequest => sender ! HttpResponse(status = StatusCodes.NotFound, entity = "Not Found")
  }

  private[this] def updateLogLevel(uri: Uri) = {
    try {
      allowLocalhost(uri) {
        val level = changeLogLevel(uri.query.getOrElse("level", ""))
        sender ! HttpResponse(entity = s"current log level is $level")
      }
    } catch {
      case e: UnsupportedOperationException =>
        sender ! HttpResponse(entity = "Disallow remote connection.", status = StatusCodes.Forbidden)
      case e: Throwable =>
        sender ! HttpResponse(entity = "Failed.", status = StatusCodes.InternalServerError)
    }
  }

  private[this] def flatten(uri: Uri, headers: List[HttpHeader], entity: HttpEntity) = {
    log.debug("flatten request entity (utf8): {}", entity.asString(HttpCharsets.`UTF-8`))
    val client = sender()
    flattenAndResponse(uri, headers, entity) map { httpResponse =>
      log.debug("httpResponse: {}", httpResponse)
      client ! httpResponse
    }
  }

  private[this] def send(uri: Uri, headers: List[HttpHeader], entity: HttpEntity) = {
    log.debug("send request entity (utf8): {}", entity.asString(HttpCharsets.`UTF-8`))
    val client = sender()
    sendAndResponse(uri, headers, entity) map { httpResponse =>
      log.debug("httpResponse: {}", httpResponse)
      client ! httpResponse
    }
  }

  private[this] def index() = {
    val html =
      """
        |<!DOCTYPE HTML>
        |<html>
        |<body>
        |<h2>Welcome to SSP-DEDUP application console</h2>
        |<ol>
        |<li><a target="_blank" href="/buildinfo">Build Info</a></li>
        |<li><a target="_blank" href="/healthchk">Health Check</a></li>
        |<li><a target="_blank" href="/jvm">JVM Metrics</a></li>
        |<li><a target="_blank" href="/akka">Akka Metrics</a></li>
        |<li><a target="_blank" href="/cache">Cache Metrics</a></li>
        |<li><a target="_blank" href="/info?pid=x">Partition x Metrics</a> (change 'pid' param  )</li>
        |<li><a target="_blank" href="/metrics">All Metrics</a></li>
        |<li><a target="_blank" href="/lastOffset?pid=x&offset=num">Update Last Offset</a> (change 'pid' and 'offset' param)</li>
        |<li><a target="_blank" href="/partitionStat">Partition State</a></li>
        |<li><a target="_blank" href="/kafka">Kafka State</a></li>
        |<li><a target="_blank" href="/config">Show Config</a></li>
        |<li><a target="_blank" href="/flatten">Convert Edge log into flattened record without sending to kafka</a> (use HTTP POST action)</li>
        |<li><a target="_blank" href="/send">Send flattened record into kafka topic 2</a> (use HTTP POST action)</li>
        |<li><a target="_blank" href="/pauseAll">Pause all partition actors</a></li>
        |<li><a target="_blank" href="/resumeAll">Resume all partition actors without reset statistics</a></li>
        |<li><a target="_blank" href="/resetAll">Reset all partition actors and their statistics</a></li>
        |<li><a target="_blank" href="/pause?pid=x">Pause partition x actor</a> (change 'pid' param)</li>
        |<li><a target="_blank" href="/resume?pid=x">Resume partition x actor</a> (change 'pid' param, it will NOT reset statistics)</li>
        |<li><a target="_blank" href="/reset?pid=x">Resume partition x actor</a> (change 'pid' param, it will reset statistics)</li>
        |<li><a target="_blank" href="/loglevel?level=ERROR">Change Log Level</a> (change 'level' param, only allow perform in localhost, valid options: ERROR, WARN, INFO, DEBUG, ALL, OFF)</li>
        |</ol>
        |
        |</body>
        |</html>
      """.stripMargin
    sender ! HttpResponse(entity = HttpEntity(`text/html`, html))
  }

  private[this] def healthchk() = {
    sender ! HttpResponse(entity = "OK")
  }

  private[this] def changeLogLevel(logLevel: String) = {
    import org.slf4j.Logger
    import org.slf4j.LoggerFactory
    import ch.qos.logback.classic.Level
    import ch.qos.logback.classic.{Logger => logbackLogger}

    LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME) match {
      case root: logbackLogger if logLevel.nonEmpty =>
        val level = Level.toLevel(logLevel, root.getLevel)
        root.setLevel(level)
        root.info(s"set current log level to $level")
        level.toString
      case root: logbackLogger =>
        root.getLevel.toString
      case x =>
        s"unsupported logger, can't configure logging: ${x.getClass}"
    }
  }

  private[this] def allowLocalhost(uri: Uri)(code: => Unit) = {
    log.info("request's ip: {}", uri.authority.host.address)
    uri.authority.host.address match {
      case "127.0.0.1" => code
      case "localhost" => code
      case "0:0:0:0:0:0:0:1" => code
      case _ => throw new UnsupportedOperationException("Disallow remote connection.")
    }
  }

  private[this] def flattenAndResponse(uri:Uri, headers:List[HttpHeader], entity: HttpEntity): Future[HttpResponse] = {
    case class FlattenException(status: StatusCode, msg: String) extends Exception(msg)
    def parseRequest(entity: HttpEntity): com.vpon.trade.Event = {
      val deserializeResult = entity.as[EdgeEvent]
      deserializeResult match {
        case Left(e: DeserializationError) => {
          val err = "rest api /flatten triggered. json deserialization error when parsing request data into EdgeEvent!!!"
          log.error(err)
          throw new FlattenException(StatusCodes.BadRequest, err)
        }
        case Right(edgeEvent) => edgeEvent.toEvent match {
          case Right(event) => event
          case Left(f: EdgeConvertFailure) => {
            val err = s"rest api /flatten triggered. bad json format: ${f.message} when convert Edge Event into Protobuf Event. "
            log.error(err)
            throw new FlattenException(StatusCodes.BadRequest, err)
          }
        }
      }
    }
    def transformResponse(flattenResult: Either[FlattenFailure, EventRecord]): Future[HttpResponse] = {
      lazy val defaultResponse = HttpResponse(StatusCodes.NoContent)
      def result = flattenResult match {
        case Right(eventRecord) => {
          HttpResponse(status = StatusCodes.OK, entity = HttpEntity(`application/json`, eventRecord.toJson.compactPrint))
        }
        case Left(f: FlattenFailure) =>
          HttpResponse(status = StatusCodes.OK, entity = HttpEntity(`text/plain`, f.message))
      }
      Future(result) recoverWith {
        case e => Future.successful(defaultResponse)
      }
    }

    val eventF: Future[com.vpon.trade.Event] = Future {
      entity match {
        case HttpEntity.NonEmpty(contentType, _) => contentType.mediaType match {
          case MediaType("application/json") => parseRequest(entity)
          case _ => {
            val err = s"rest api /flatten triggered. contentType not supported yet"
            log.error(err)
            throw new FlattenException(StatusCodes.UnsupportedMediaType, err)
          }
        }
        case _ => {
          val err = s"rest api /flatten triggered. HttpEntity empty"
          log.error(err)
          throw new FlattenException(StatusCodes.BadRequest, err)
        }
      }
    }

    val f = for {
      event <- eventF
      flattenResult <- Flattener.getInstance.convert(event.toByteArray(), true)
      response <- transformResponse(flattenResult)
    } yield response

    f.recover {
      case e: FlattenException => HttpResponse(status = e.status, e.msg)
      case e: Throwable => {
        val err = s"rest api /flatten triggered. ${ExceptionUtils.getStackTrace(e)}"
        log.error(err)
        HttpResponse(status = StatusCodes.BadRequest, err)
      }
    }
  }

  private[this] def sendAndResponse(uri:Uri, headers:List[HttpHeader], entity: HttpEntity): Future[HttpResponse] = {
    case class SendException(status: StatusCode, msg: String) extends Exception(msg)
    def parseRequest(entity: HttpEntity): EventRecord = {
      val deserializeResult = entity.as[EventRecord]
      deserializeResult match {
        case Left(e: DeserializationError) => {
          val err = s"rest api /send triggered. json deserialization error when parsing request data into EventRecord"
          log.error(err)
          throw new SendException(StatusCodes.BadRequest, err)
        }
        case Right(eventRecord) => eventRecord
      }
    }

    def transformResponse(eventRecord: EventRecord): HttpResponse = {
      val targetKafkaProducer = new CustomPartitionProducer[String, String](flattenEventsBrokers, "kafka.serializer.StringEncoder", true)
      val keyedMessage = new KeyedMessage(flattenEventsTopic, eventRecord.event_key, eventRecord.toJson.compactPrint)
      try {
        targetKafkaProducer.sendMessages(Seq(keyedMessage))
        HttpResponse(status = StatusCodes.OK, entity = HttpEntity(`text/plain`, "Success sent event record to kafka topic 2!"))
      } catch {
        case e: Throwable => {
          val err = s"rest api /send triggered. failed to send event record to kafka topic 2!\n${ExceptionUtils.getStackTrace(e)}"
          log.error(err)
          throw new SendException(StatusCodes.InternalServerError, err)
        }
      }
    }

    Future {
      val eventRecord = entity match {
        case HttpEntity.NonEmpty(contentType, _) => contentType.mediaType match {
          case MediaType("application/json") => parseRequest(entity)
          case _ => {
            val err = s"rest api /send triggered. contentType not supported yet"
            log.error(err)
            throw new SendException(StatusCodes.UnsupportedMediaType, err)
          }
        }
        case _ => {
          val err = s"rest api /send triggered. HttpEntity empty"
          log.error(err)
          throw new SendException(StatusCodes.BadRequest, err)
        }
      }
      transformResponse(eventRecord)
    }.recover{
      case e: SendException => HttpResponse(status = e.status, e.msg)
      case e: Throwable => {
        val err = s"rest api /send triggered. ${ExceptionUtils.getStackTrace(e)}"
        log.error(err)
        HttpResponse(status = StatusCodes.BadRequest, err)
      }
    }
  }

  private[this] def buildInfo() = {
    val buildInfoFileName = "buildinfo.properties"
    val buildInfoFileContent: Option[String] = Try{
      val url = classLoader.getResource(buildInfoFileName)
      Source.fromURL(url).getLines().mkString
    }.toOption
    val buildInfo = buildInfoFileContent.getOrElse(s"Not found $buildInfoFileName")
    sender ! HttpResponse(entity = HttpEntity(`text/plain`, buildInfo))
  }

  private[this] def showConfig() = {
    val render = config2Json
    sender ! HttpResponse(entity = HttpEntity(`application/json`, render.compactPrint))
  }

  private[this] def reportKafka() = {
    val s = sender()
    val partitionMaster = context.system.actorSelection("/user/dedup-partitions")

    val askKafkaConnectionFuture = ask(partitionMaster, GetKafkaConnection).mapTo[String]
    askKafkaConnectionFuture onComplete {
      case Success(x) => {
        val kafka = JsObject("kafka" -> JsString(x))
        s ! HttpResponse(entity = HttpEntity(`application/json`, kafka.compactPrint))
      }
      case Failure(ex) => {
        s ! HttpResponse(status = StatusCodes.RequestTimeout, entity = "Akka Timed Out")
      }
    }
  }

  private[this] def reportMetrics() = {
    val s = sender()
    val defaultFields: Map[String, JsValue] = Map("jvm" -> jvmMBeansStatsPresentation, "akka" -> akkaMBeansStatsPresentation, "cache" -> allCacheStatsPresentation)
    val partitionMaster = context.system.actorSelection("/user/dedup-partitions")
    val askPartitionMetricsFuture = ask(partitionMaster, ReportPartitionStat)
    askPartitionMetricsFuture.foreach { case PartitionStat(partitions) =>
      val sortedPartitions = partitions.toSeq.sorted
      Future.sequence{
        sortedPartitions.map(p => {
          log.debug(s"===> partition=$p")
          val metrics = context.system.actorSelection(s"/user/dedup-partitions/dedup-${p}/dedup-metrics-${p}")
          log.debug(s"===> metrics=$metrics")
          ask(metrics, GetMetrics).mapTo[JsObject]
        })
      } onComplete {
        case Success(x) => {
          val allActorsMap = x map (metrics => {
            val fieldValue = metrics.getFields("total")
            fieldValue.isEmpty match {
              case true => Map.empty[String, BigDecimal]
              case false => {
                val jsObject = fieldValue.head.asInstanceOf[JsObject]
                jsObject.fields.mapValues(jsValue => jsValue.asInstanceOf[JsNumber].value)
              }
            }
          })
          val summaryMap: Map[String, JsValue] = mergeMap(allActorsMap)((v1, v2) => v1 + v2).map(m => m._1 -> JsNumber(m._2.toLong))
          val summaryFields = Map("summary" -> JsObject(summaryMap))

          def getPartitionId(s: JsObject): Option[Int] = {
            val fieldValue = s.getFields("partitionId")
            fieldValue.isEmpty match {
              case true => None
              case false => Some(fieldValue.head.asInstanceOf[JsNumber].value.intValue())
            }
          }

          def getPartitionKey(partitionId: Int): String = s"partition-$partitionId"

          val partitionItemFields: Map[String, JsValue] = {
            val partitionIdMap: Map[Option[Int], JsValue] = (x map (s => getPartitionId(s) -> s) toMap)
            val tempIt = partitionIdMap map {
              case (k: Option[Int], v: JsValue) => k match {
                case None => None
                case Some(t) => Some(getPartitionKey(t) -> v)
              }
            }
            ListMap(tempIt.flatten.toSeq.sortBy(_._1):_*)
          }
          val partitionFields: Map[String, JsValue] = Map("partitions" -> JsObject(partitionItemFields))
          val allFields: Map[String, JsValue] = defaultFields ++ summaryFields ++ partitionFields
          s ! HttpResponse(entity = HttpEntity(`application/json`, JsObject(allFields).compactPrint))
        }
        case Failure(ex) => {
          log.error(s"===> AskGetMetricsTimeoutException")
          s ! HttpResponse(entity = HttpEntity(`application/json`, JsObject(defaultFields).compactPrint))
        }
      }
    }
    askPartitionMetricsFuture.onFailure {
      case e: AskTimeoutException =>
        log.error(s"===> AskTimeoutException")
        s ! HttpResponse(status = StatusCodes.RequestTimeout, entity = "Akka Timed Out")
    }
  }

  private[this] def reportPartitionStat() = {
    val s = sender()
    val partitionMaster = context.system.actorSelection("/user/dedup-partitions")
    val askFuture = ask(partitionMaster, ReportPartitionStat)
    askFuture.foreach { case PartitionStat(partitions) =>
      val sortedPartitions = partitions.toSeq.sorted
      val jsonResponse = write(sortedPartitions)
      s ! HttpResponse(entity = HttpEntity(`application/json`, jsonResponse))
    }
    askFuture.onFailure {
      case e: AskTimeoutException =>
        s ! HttpResponse(status = StatusCodes.RequestTimeout, entity = "Akka Timed Out")
    }
  }

  private[this] def updateLastOffset(uri:Uri) = {
    val s = sender()
    val partitionId = uri.query.getOrElse("pid", "")
    if (partitionId.isEmpty) {
      s ! HttpResponse(entity = HttpEntity(`text/plain`, s"Can not get query parameter 'pid' value"))
    }

    try {
      partitionId.toInt
    } catch {
      case e:NumberFormatException => s ! HttpResponse(entity = HttpEntity(`application/json`, s"Query parameter 'pid' value is NOT an INT number"))
    }

    val offset = uri.query.getOrElse("offset", "")
    if (offset.isEmpty) {
      s ! HttpResponse(entity = HttpEntity(`text/plain`, s"Can not get query parameter 'offset' value"))
    }

    try {
      offset.toInt
    } catch {
      case e:NumberFormatException => s ! HttpResponse(entity = HttpEntity(`application/json`, s"Query parameter 'offset' value is NOT an INT number"))
    }

    val partitionActor = context.system.actorSelection(s"/user/dedup-partitions/dedup-${partitionId}")
    partitionActor ! PauseWork

    val offsetBucketWithKeyPrefix = CBExtension(context.system).buckets("offset")
    val offsetBucket              = offsetBucketWithKeyPrefix.bucket
    val offsetBucketKeyPrefix     = offsetBucketWithKeyPrefix.keyPrefix
    val cbOffsetKey               = s"${offsetBucketKeyPrefix}${partitionId}"
    try {
      offsetBucket.upsert(StringDocument.create(cbOffsetKey, offset))
      s ! HttpResponse(entity = HttpEntity(`text/plain`, s"Successfully update offset $offset on the partition ${partitionId}"))
    } catch {
      case e: Exception => s ! HttpResponse(entity = HttpEntity(`text/plain`, s"Failed to update offset $offset on the partition ${partitionId}"))
    } finally {
      partitionActor ! ResumeWork
    }
  }

  private[this] def operateAll(cmd: Command) = {
    val s = sender()
    val (message, action) = cmd match {
      case Pause => (PauseWork, "paused")
      case Resume => (ResumeWork, "resumed")
      case Reset => (ResetWork, "reseted")
      case _ => s ! HttpResponse(entity = HttpEntity("only accept Pause or Resume or Reset action"))
    }
    val partitionMaster = context.system.actorSelection("/user/dedup-partitions")
    val askFuture = ask(partitionMaster, ReportPartitionStat)
    askFuture.foreach { case PartitionStat(partitions) =>
      val sortedPartitions = partitions.toSeq.sorted
      sortedPartitions.foreach(p => {
        val partitionActor = context.system.actorSelection(s"/user/dedup-partitions/dedup-${p}")
        partitionActor ! message
      })
      s ! HttpResponse(entity = HttpEntity(`text/plain`, s"Dedup service was ${action} on this node"))
    }
    askFuture.onFailure {
      case e: AskTimeoutException =>
        s ! HttpResponse(status = StatusCodes.RequestTimeout, entity = "Akka Timed Out")
    }
  }

  private[this] def operate(uri:Uri, cmd:Command) = {
    val s = sender()
    val (message, action) = cmd match {
      case Pause => (PauseWork, "paused")
      case Resume => (ResumeWork, "resumed")
      case Reset => (ResetWork, "reseted")
      case _ => s ! HttpResponse(entity = HttpEntity("only accept Pause or Resume or Reset action"))
    }
    val partitionId = uri.query.getOrElse("pid", "")
    if (!partitionId.isEmpty) {
      try {
        partitionId.toInt
      } catch {
        case e:NumberFormatException => s ! HttpResponse(entity = HttpEntity(`application/json`, s"Query parameter 'pid' value is NOT an INT number"))
      }
      val partitionActor = context.system.actorSelection(s"/user/dedup-partitions/dedup-${partitionId}")
      partitionActor ! message
      s ! HttpResponse(entity = HttpEntity(`text/plain`, s"Dedup service was ${action} on the partition ${partitionId}"))
    } else {
      s ! HttpResponse(entity = HttpEntity(`text/plain`, s"Can not get query parameter 'pid' value"))
    }
  }

  private[this] def info(uri:Uri) = {
    val s = sender()
    val partitionId = uri.query.getOrElse("pid", "")
    if (!partitionId.isEmpty) {
      try {
        partitionId.toInt
      } catch {
        case e:NumberFormatException => s ! HttpResponse(entity = HttpEntity(`application/json`, s"Query parameter 'pid' value is NOT a INT number"))
      }
      log.debug("======> partitionId: " + partitionId)
      val metrics = context.system.actorSelection(s"/user/dedup-partitions/dedup-${partitionId}/dedup-metrics-${partitionId}")
      log.debug("======> metrics: " + metrics)
      ask(metrics, GetInfo).mapTo[JsObject] onComplete {
        case Success(x) => {
          log.debug("======> x: " + x)
          s ! HttpResponse(entity = HttpEntity(`application/json`, x.compactPrint))
        }
        case Failure(ex) => {
          s ! HttpResponse(entity = HttpEntity(`application/json`, s"Ask GetInfo message of PartitionMetrics-$partitionId timed out"))
        }
      }
    } else {
      s ! HttpResponse(entity = HttpEntity(`application/json`, s"Can not get query parameter 'pid' value"))
    }
  }

  private[this] def shutdown(): Unit = {
    val s = sender()
    ask(httpListener, Unbind(10.second))(10.second).onComplete { _ =>
      s ! ShutdownCompleted
    }
  }
}

