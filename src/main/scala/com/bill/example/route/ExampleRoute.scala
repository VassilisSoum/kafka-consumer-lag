package com.bill.example.route

import cats.effect.IO
import com.bill.example.service.KafkaConsumerService
import org.http4s.Method.GET
import org.http4s.dsl.io._
import org.http4s.{HttpRoutes, Response, Status}

class ExampleRoute(kafkaConsumerService: KafkaConsumerService) {

  val healthCheckService: HttpRoutes[IO] = HttpRoutes.of[IO] { case GET -> Root / "health" =>
    val lag = kafkaConsumerService.getLag
    val isStable = kafkaConsumerService.isLagStable

    if (isStable) {
      lag.map(value => Response(Status.InternalServerError).withEntity(s"Lag has been stable: $value"))
    } else {
      lag.map(value => Response(Status.Ok).withEntity(s"Lag is not stable: $value"))
    }
  }

}
