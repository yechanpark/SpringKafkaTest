package com.example.springkafkatest.consumer

import com.example.springkafkatest.dto.Dto
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
import org.springframework.kafka.support.serializer.JsonDeserializer

@Configuration
@EnableKafka
class KafkaConsumerConfig {

    @Value("\${kafka.servers}")
    lateinit var kafkaServers: String

    @Value("\${kafka.group-id}")
    lateinit var kafkaGroupId: String

    @Bean
    fun kafkaConsumerProps(): Map<String, Any> {
        return mapOf(
            // bootstrap.servers
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaServers,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,

            // group.id
            ConsumerConfig.GROUP_ID_CONFIG to kafkaGroupId,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,

            // enable.auto.commit
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false
        )
    }

    @Bean
    fun kafkaContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, Dto> {
        val deserializer = JsonDeserializer(Dto::class.java)
        deserializer.setRemoveTypeHeaders(false)
        deserializer.addTrustedPackages("*")
        deserializer.setUseTypeMapperForKey(true)

        return ConcurrentKafkaListenerContainerFactory<String, Dto>().apply {
            consumerFactory = DefaultKafkaConsumerFactory(
                kafkaConsumerProps(),
                ErrorHandlingDeserializer(StringDeserializer()),
                ErrorHandlingDeserializer(deserializer)
            )
            containerProperties.ackMode = ContainerProperties.AckMode.MANUAL
        }
    }
}