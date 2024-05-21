package com.bill.example.service

import cats.effect.{IO, Resource}
import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, ConsumerRecord, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition

import java.time.Duration
import java.util.Collections
import scala.collection.mutable
import scala.jdk.CollectionConverters._

class KafkaConsumerService(consumer: KafkaConsumer[String, String]) {

  private val partitionLagMap: mutable.Map[Int, Long] = mutable.Map()
  private val lagHistory: mutable.Queue[Long] = mutable.Queue()
  private val gracePeriodMs: Long = 30000 // 30 seconds grace period
  private val historyResetPeriodMs: Long = 120000 // 120 seconds to reset history
  private val maxNonConsumingPolls: Int = 5 // Number of consecutive polls with no consumption to consider lag stable

  @volatile private var lastRebalanceTime: Long = System.currentTimeMillis()
  @volatile private var lastHistoryResetTime: Long = System.currentTimeMillis()
  @volatile private var lastConsumedTime: Long = System.currentTimeMillis()
  @volatile private var nonConsumingPolls: Int = 0

  consumer.subscribe(Collections.singletonList("test_topic"), new CustomRebalanceListener())

  /** Start consuming messages from Kafka.
    * This method will poll for records, process them, and commit the offsets.
    * It will also reset the lag history periodically.
    * This method will run indefinitely.
    * @return Unit
    */
  def startConsuming: IO[Unit] = {
    val consume: IO[Unit] = for {
      records <- pollRecords
      _ <- processRecords(records)
      _ <- resetLagHistoryPeriodically
    } yield ()

    consume.foreverM
  }

  /** Poll for records from Kafka.
    * @return
    */
  private def pollRecords: IO[Iterable[ConsumerRecord[String, String]]] = IO {
    consumer.poll(Duration.ofSeconds(3)).asScala
  }

  /** Process the records. This method will simulate slow processing by introducing a delay.
    * It will also commit the offsets.
    * It will update the partition lag map and lag history.
    * @param records The records to process
    * @return Unit
    */
  private def processRecords(records: Iterable[ConsumerRecord[String, String]]): IO[Unit] = IO {
    if (records.isEmpty) {
      nonConsumingPolls += 1
    } else {
      nonConsumingPolls = 0
      lastConsumedTime = System.currentTimeMillis()
    }

    records.foreach { record =>
      println(s"Received message: ${record.value()} from partition: ${record.partition()}")

      // Simulate slow processing by introducing a delay
      Thread.sleep(2000) // 2 seconds delay

      val partition = record.partition()
      val currentOffset = record.offset()
      val latestOffset = consumer
        .endOffsets(Collections.singleton(new TopicPartition(record.topic(), partition)))
        .get(new TopicPartition(record.topic(), partition))
      val lag = latestOffset - currentOffset
      partitionLagMap(partition) = lag

      // Add the lag to history
      if (lagHistory.size >= 10) lagHistory.dequeue()
      lagHistory.enqueue(lag)

      consumer.commitSync(
        Collections.singletonMap(
          new TopicPartition(record.topic(), record.partition()),
          new OffsetAndMetadata(currentOffset + 1)
        )
      )
    }
  }

  /** Reset the lag history periodically to avoid stale data when a rebalance does not happen.
    * @return Unit
    */
  private def resetLagHistoryPeriodically: IO[Unit] = IO {
    val now = System.currentTimeMillis()
    if (now - lastHistoryResetTime > historyResetPeriodMs) {
      lagHistory.clear()
      lastHistoryResetTime = now
    }
  }

  /** Get the total lag across all partitions.
    * @return The total lag
    */
  def getLag: IO[Long] = IO(partitionLagMap.values.sum)

  /** Check if the lag is stable across all partitions.
    * @return True if the lag is stable, false otherwise
    */
  def isLagStable: Boolean = {
    val now = System.currentTimeMillis()
    if (now - lastRebalanceTime < gracePeriodMs) {
      println("In grace period, skipping lag stability check.")
      false
    } else if (now - lastConsumedTime > gracePeriodMs) {
      println("No messages consumed for longer than the grace period, considering lag stable.")
      true
    } else {
      nonConsumingPolls >= maxNonConsumingPolls || lagHistory.distinct.size == 1
    }
  }

  /** Shutdown the consumer.
    * @return
    */
  def shutdown: IO[Unit] = IO {
    consumer.wakeup()
    consumer.close()
  }

  private class CustomRebalanceListener extends ConsumerRebalanceListener {

    override def onPartitionsRevoked(partitions: java.util.Collection[TopicPartition]): Unit = {
      println("Committing offsets before rebalancing...")
      consumer.commitSync()
    }

    override def onPartitionsAssigned(partitions: java.util.Collection[TopicPartition]): Unit = {
      println(s"Rebalanced: ${partitions.asScala.mkString(", ")}")
      lastRebalanceTime = System.currentTimeMillis()

      // Reset the lag history and partition lag map
      lagHistory.clear()
      partitionLagMap.clear()
    }
  }
}

object KafkaConsumerService {

  def resource(consumer: KafkaConsumer[String, String]): Resource[IO, KafkaConsumerService] =
    Resource.make {
      IO(new KafkaConsumerService(consumer))
    } { service =>
      service.shutdown
    }
}
