package com.vpon.ssp.report.replay

import java.util.Properties

import scala.util.Try

import kafka.producer.{Partitioner, KeyedMessage, Producer, ProducerConfig}
import kafka.utils.VerifiableProperties
import org.slf4j.LoggerFactory

class CustomPartitionProducer[K, V](brokers: String) {

  val props = new Properties()
  props.put("metadata.broker.list", brokers)
  props.put("request.required.acks", "1")
  props.put("key.serializer.class", "kafka.serializer.StringEncoder")
  props.put("serializer.class", "kafka.serializer.DefaultEncoder")
  props.put("partitioner.class", "com.vpon.ssp.report.replay.ConsistentPartitioner")

  val config = new ProducerConfig(props)
  val producer = new Producer[K, V](config)

  // KeyedMessage(topic, key, value)
  def sendMessages(messages: Seq[KeyedMessage[K, V]]): Unit = {
    producer.send(messages.toArray:_*)
  }
}

class ConsistentPartitioner(val props: VerifiableProperties) extends Partitioner {
  val logger = LoggerFactory.getLogger("ConsistentPartitioner")
  /**
   * Take key as value and return the partition number, "t_xxx_1"
   */
  override def partition(key : scala.Any, numPartitions : scala.Int) : scala.Int = {
    val keyString = key.asInstanceOf[String]
    val items = keyString.split("_")
    val partitionString = if (items.length >= 2) items(items.length - 1) else ""
    val partitionId = Try{partitionString.toInt}.toOption.getOrElse(0)
    val partition = partitionId % numPartitions
    logger.debug(s"keyString = ${keyString}, partitionId = ${partitionId}, numPartitions = ${numPartitions}, partition = ${partition}")
    partition
  }
}
