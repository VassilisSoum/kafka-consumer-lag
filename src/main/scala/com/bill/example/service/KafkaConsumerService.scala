package com.bill.example.service

import cats.effect.{IO, Resource}
import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, ConsumerRecord, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition

import java.time.Duration
import java.util.Collections
import java.util.concurrent.Executors
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}
import scala.jdk.CollectionConverters._

/** KafkaConsumerService is a class that encapsulates the logic for consuming messages from a Kafka topic.
  * It uses a single-threaded executor to poll and process records from the Kafka consumer.
  * It also keeps track of the lag for each partition and provides a method to check if the lag is stable.
  *
  * @param consumer KafkaConsumer instance used to consume messages.
  */
class KafkaConsumerService(consumer: KafkaConsumer[String, String]) {

  // Single-threaded executor used for blocking operations.
  private val singleThreadExecutor: ExecutionContextExecutorService =
    ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor())

  // Map to keep track of the lag for each partition.
  private val partitionLagMap: mutable.Map[Int, Long] = mutable.Map()

  // Queue to keep track of the lag history.
  private val lagHistory: mutable.Queue[Long] = mutable.Queue()

  // Grace period in milliseconds after a rebalance event during which lag stability checks are skipped.
  private val gracePeriodMs: Long = 30000

  // Period in milliseconds after which the lag history is reset.
  private val historyResetPeriodMs: Long = 120000

  // Number of consecutive polls with no consumption after which the lag is considered stable.
  private val maxNonConsumingPolls: Int = 5

  // Timestamps and counters used to track the state of the consumer.
  @volatile private var lastRebalanceTime: Long = System.currentTimeMillis()
  @volatile private var lastHistoryResetTime: Long = System.currentTimeMillis()
  @volatile private var lastConsumedTime: Long = System.currentTimeMillis()
  @volatile private var nonConsumingPolls: Int = 0

  // Subscribe to the Kafka topic and set the custom rebalance listener.
  consumer.subscribe(Collections.singletonList("test_topic"), new CustomRebalanceListener())

  /** Starts consuming messages from the Kafka topic.
    * This method polls records from the Kafka consumer, processes them, and resets the lag history periodically.
    *
    * @return IO[Unit] representing the consuming process.
    */
  def startConsuming: IO[Unit] = {
    val consume: IO[Unit] = for {
      records <- pollRecords
      _ <- processRecords(records)
      _ <- resetLagHistoryPeriodically
    } yield ()

    consume.foreverM
  }

  // Polls records from the Kafka consumer.
  private def pollRecords: IO[Iterable[ConsumerRecord[String, String]]] = IO
    .blocking {
      consumer.poll(Duration.ofSeconds(3)).asScala
    }
    .evalOn(singleThreadExecutor)

  /*
  Processes records by updating the lag for each partition, updating the lag history,
  and committing the record offset.
   */
  private def processRecords(records: Iterable[ConsumerRecord[String, String]]): IO[Unit] = IO
    .blocking {
      updateNonConsumingPolls(records)
      records.foreach(processRecord)
    }
    .evalOn(singleThreadExecutor)

  /*
    Updates the number of consecutive polls with no consumption
    and the timestamp of the last consumed message.
   */
  private def updateNonConsumingPolls(records: Iterable[ConsumerRecord[String, String]]): Unit =
    if (records.isEmpty) {
      nonConsumingPolls += 1
    } else {
      nonConsumingPolls = 0
      lastConsumedTime = System.currentTimeMillis()
    }

  /*
    Processes a single record by calculating the lag, updating the partition lag map,
    updating the lag history, and committing the record offset.
   */
  private def processRecord(record: ConsumerRecord[String, String]): Unit = {
    Thread.sleep(2000)

    val partition = record.partition()
    val currentOffset = record.offset()
    val latestOffset = getLatestOffset(record, partition)
    val lag = latestOffset - currentOffset

    updatePartitionLag(partition, lag)
    updateLagHistory(lag)
    commitRecordOffset(record, currentOffset)
  }

  /*
    Returns the latest offset for a given record and partition.
   */
  private def getLatestOffset(record: ConsumerRecord[String, String], partition: Int): Long =
    consumer
      .endOffsets(Collections.singleton(new TopicPartition(record.topic(), partition)))
      .get(new TopicPartition(record.topic(), partition))

  /*
    Updates the lag for a given partition in the partition lag map.
   */
  private def updatePartitionLag(partition: Int, lag: Long): Unit =
    partitionLagMap(partition) = lag

  /*
    Updates the lag history by adding the latest lag and removing
    the oldest lag if the history size exceeds 10.
   */
  private def updateLagHistory(lag: Long): Unit = {
    if (lagHistory.size >= 10) lagHistory.dequeue()
    lagHistory.enqueue(lag)
  }

  /*
    Commits the record offset by incrementing the current offset by 1.
   */
  private def commitRecordOffset(record: ConsumerRecord[String, String], currentOffset: Long): Unit =
    consumer.commitSync(
      Collections.singletonMap(
        new TopicPartition(record.topic(), record.partition()),
        new OffsetAndMetadata(currentOffset + 1)
      )
    )

  // Resets the lag history periodically.
  private def resetLagHistoryPeriodically: IO[Unit] = IO
    .blocking {
      val now = System.currentTimeMillis()
      if (now - lastHistoryResetTime > historyResetPeriodMs) {
        lagHistory.clear()
        lastHistoryResetTime = now
      }
    }
    .evalOn(singleThreadExecutor)

  /** Returns the total lag across all partitions.
    *
    * @return IO[Long] representing the total lag.
    */
  def getLag: IO[Long] = IO(partitionLagMap.values.sum).evalOn(singleThreadExecutor)

  /** Checks if the lag is stable.
    * The lag is considered stable if no messages have been consumed for longer than the grace period,
    * or if the number of non-consuming polls has reached the maximum, or if all lags in the history are the same.
    *
    * @return Boolean indicating whether the lag is stable.
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

  /** Shuts down the Kafka consumer and the executor service.
    *
    * @return IO[Unit] representing the shutdown process.
    */
  def shutdown: IO[Unit] = IO
    .blocking {
      consumer.close()
      singleThreadExecutor.shutdown()
    }
    .evalOn(singleThreadExecutor)

  /*
  Custom rebalance listener that commits offsets before rebalancing and
  resets the lag history and map after rebalancing.
   */
  private class CustomRebalanceListener extends ConsumerRebalanceListener {

    override def onPartitionsRevoked(partitions: java.util.Collection[TopicPartition]): Unit = {
      println("Committing offsets before rebalancing...")
      consumer.commitSync()
    }

    override def onPartitionsAssigned(partitions: java.util.Collection[TopicPartition]): Unit = {
      println(s"Rebalanced: ${partitions.asScala.mkString(", ")}")
      lastRebalanceTime = System.currentTimeMillis()

      // Reset the lag history and partition lag map.
      lagHistory.clear()
      partitionLagMap.clear()
    }
  }
}

/** Companion object for the KafkaConsumerService class.
  * Provides a method to create a Resource of KafkaConsumerService.
  */
object KafkaConsumerService {

  /** Creates a Resource of KafkaConsumerService.
    * The resource acquisition process creates a new KafkaConsumerService,
    * and the resource release process shuts down the KafkaConsumerService.
    *
    * @param consumer KafkaConsumer instance used to consume messages.
    * @return Resource[IO, KafkaConsumerService] representing the KafkaConsumerService.
    */
  def resource(consumer: KafkaConsumer[String, String]): Resource[IO, KafkaConsumerService] =
    Resource.make {
      IO(new KafkaConsumerService(consumer))
    } { service =>
      service.shutdown
    }
}
