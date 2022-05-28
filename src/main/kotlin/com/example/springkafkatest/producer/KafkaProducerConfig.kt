package com.example.springkafkatest.producer

import com.example.springkafkatest.dto.Dto
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.support.serializer.JsonSerializer

@Configuration
@EnableKafka
class KafkaProducerConfig(
    @Value("\${kafka.servers}") private val kafkaServers: String
) {
    fun kafkaProducerProps() = mapOf<String, Any>(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaServers,
        ProducerConfig.RETRIES_CONFIG to 0,
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to JsonSerializer::class.java
    )

    @Bean
    fun kafkaProducerFactory(): ProducerFactory<String, Dto> = DefaultKafkaProducerFactory(kafkaProducerProps())

    @Bean
    fun kafkaTemplate(): KafkaTemplate<String, Dto> = KafkaTemplate(kafkaProducerFactory())
}