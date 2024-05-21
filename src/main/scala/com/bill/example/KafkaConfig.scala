package com.bill.example

final case class KafkaConfig private (
    bootstrapServerConfig: String,
    groupIdConfig: String,
    keyDeserializerClassConfig: String,
    valueDeserializerClassConfig: String,
    autoOffsetResetConfig: String,
    enableAutoCommitConfig: String
)

object KafkaConfig {

  def create(): KafkaConfig = {
    val environmentVars = sys.env
    KafkaConfig(
      bootstrapServerConfig = environmentVars.getOrElse("BOOTSTRAP_SERVER", "localhost:9092"),
      groupIdConfig = environmentVars.getOrElse("GROUP_ID", "group_id"),
      keyDeserializerClassConfig =
        environmentVars.getOrElse("KEY_DESERIALIZER_CLASS", "org.apache.kafka.common.serialization.StringDeserializer"),
      valueDeserializerClassConfig = environmentVars
        .getOrElse("VALUE_DESERIALIZER_CLASS", "org.apache.kafka.common.serialization.StringDeserializer"),
      autoOffsetResetConfig = environmentVars.getOrElse("AUTO_OFFSET_RESET", "earliest"),
      enableAutoCommitConfig = environmentVars.getOrElse("ENABLE_AUTO_COMMIT", "false")
    )
  }
}
