package com.bill.example

import cats.effect.IO
import com.bill.example.route.ExampleRoute
import com.bill.example.service.KafkaConsumerService
import com.comcast.ip4s.IpLiteralSyntax
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.server.middleware.Logger

import java.util.Properties

object ExampleServer {

  def run: IO[Unit] =
    for {
      kafkaConfig <- IO(KafkaConfig.create())
      kafkaProperties = createKafkaProperties(kafkaConfig)
      consumer = new KafkaConsumer[String, String](kafkaProperties)
      resource <- KafkaConsumerService.resource(consumer).use { kafkaConsumerService =>
        for {
          _ <- kafkaConsumerService.startConsuming.start // Start another fiber to not block the main one
          routes <- IO(new ExampleRoute(kafkaConsumerService).healthCheckService)
          httpApp = Logger.httpApp(logHeaders = true, logBody = true)(routes.orNotFound)
          server <- EmberServerBuilder
            .default[IO]
            .withHost(ipv4"0.0.0.0")
            .withPort(port"8080")
            .withHttpApp(httpApp)
            .build
            .use(_ => IO.never)
        } yield server
      }
    } yield resource

  private def createKafkaProperties(kafkaConfig: KafkaConfig): Properties = {
    val props: Properties = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.bootstrapServerConfig)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaConfig.groupIdConfig)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, kafkaConfig.keyDeserializerClassConfig)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, kafkaConfig.valueDeserializerClassConfig)
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, kafkaConfig.enableAutoCommitConfig)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafkaConfig.autoOffsetResetConfig)
    props
  }

}
