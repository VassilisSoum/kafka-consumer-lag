/*package com.bill.example

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties
import scala.util.Random

object KafkaMessageProducer extends App {
  val bootstrapServers = "localhost:9093"
  val topic = "test_topic"

  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

  val producer = new KafkaProducer[String, String](props)

  sys.addShutdownHook {
    producer.close()
  }

  val random = new Random()

  while (true) {
    val key = "key" + random.nextInt(100)
    val value = "value" + random.nextInt(100)

    val record = new ProducerRecord[String, String](topic, key, value)
    producer.send(record)

    println(s"Sent message: key = $key, value = $value")

    Thread.sleep(1000)
  }
}*/
