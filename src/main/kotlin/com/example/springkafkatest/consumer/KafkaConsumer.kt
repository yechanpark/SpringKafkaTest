package com.example.springkafkatest.consumer

import com.example.springkafkatest.dto.Dto
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Component


@Component
class KafkaConsumer {
    @KafkaListener(topics = ["\${kafka.topic}"], containerFactory = "kafkaContainerFactory")
    fun consume(
        @Payload payload: Dto,
        @Header(KafkaHeaders.RECEIVED_TOPIC) topic: String,
        @Header(KafkaHeaders.RECEIVED_TIMESTAMP) timestamp: Long,
        @Header(KafkaHeaders.OFFSET) offsets: Int,
        a: ConsumerRecord<String, Dto>,
        ack: Acknowledgment
    ) {
        println("raw data: $a")
        println("offset: $offsets")
        if (offsets < 2) {
            println("acknowledge()")
            ack.acknowledge()
        } else {
            println("nack()")
            // nack을 호출하지 않으면 offset이 commit됨
            // nack을 호출하면 해당 offset부터 재처리
            ack.nack(1000)
        }

    }
}