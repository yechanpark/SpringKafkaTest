package com.example.springkafkatest.consumer

import com.example.springkafkatest.dto.Dto
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.context.annotation.Profile
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Component
import java.time.LocalDateTime
import java.time.ZoneId
import java.util.concurrent.CountDownLatch


@Component
@Profile("!test")
class KafkaConsumer {
    @KafkaListener(topics = ["\${kafka.topic}"], containerFactory = "kafkaContainerFactory")
    fun consume1(
        @Payload payload: Dto,
        @Header(KafkaHeaders.RECEIVED_TOPIC) topic: String,
        @Header(KafkaHeaders.RECEIVED_TIMESTAMP) timestamp: Long,
        @Header(KafkaHeaders.OFFSET) offsets: Int,
        a: ConsumerRecord<String, Dto>,
        ack: Acknowledgment
    ): Boolean {
        println("[Consumer1] raw data: $a")
        println("[Consumer1] offset: $offsets")

        val oneHourAgoTimestamp = LocalDateTime.now().minusHours(1).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()

        println("[Consumer1] payload.timestamp: ${payload.timestamp}")
        println("[Consumer1] oneHourAgoTimestamp: $oneHourAgoTimestamp")

        // 1시간이 지난 데이터만 처리
        return if (payload.timestamp < oneHourAgoTimestamp) {
            println("[Consumer2] acknowledge()")
            ack.acknowledge()
            true
        } else {
            println("[Consumer2] nack()")
            // nack을 호출하지 않으면 offset이 commit됨
            // nack을 호출하면 해당 offset부터 재처리
            ack.nack(1000)
            false
        }

    }

    @KafkaListener(topics = ["\${kafka.topic}"], containerFactory = "kafkaContainerFactory")
    fun consume2(
        @Payload payload: Dto,
        @Header(KafkaHeaders.RECEIVED_TOPIC) topic: String,
        @Header(KafkaHeaders.RECEIVED_TIMESTAMP) timestamp: Long,
        @Header(KafkaHeaders.OFFSET) offsets: Int,
        a: ConsumerRecord<String, Dto>,
        ack: Acknowledgment
    ): Boolean {
        println("[Consumer2] raw data: $a")
        println("[Consumer2] offset: $offsets")

        val oneHourAgoTimestamp = LocalDateTime.now().minusHours(1).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()

        println("[Consumer2] payload.timestamp: ${payload.timestamp}")
        println("[Consumer2] oneHourAgoTimestamp: $oneHourAgoTimestamp")

        // 1시간이 지난 데이터만 처리
        return if (payload.timestamp < oneHourAgoTimestamp) {
            println("[Consumer2] acknowledge()")
            ack.acknowledge()
            true
        } else {
            println("[Consumer2] nack()")
            // nack을 호출하지 않으면 offset이 commit됨
            // nack을 호출하면 해당 offset부터 재처리
            ack.nack(1000)
            false
        }

    }
}


@Component
@Profile("test")
class KafkaConsumerForTest: KafkaConsumer() {
    private val latch = CountDownLatch(1)

    @KafkaListener(topics = ["\${kafka.topic}"], containerFactory = "kafkaContainerFactory")
    override fun consume1(
        @Payload payload: Dto,
        @Header(KafkaHeaders.RECEIVED_TOPIC) topic: String,
        @Header(KafkaHeaders.RECEIVED_TIMESTAMP) timestamp: Long,
        @Header(KafkaHeaders.OFFSET) offsets: Int,
        a: ConsumerRecord<String, Dto>,
        ack: Acknowledgment
    ): Boolean {
        val result = super.consume1(payload, topic, timestamp, offsets, a, ack)
        if ( result ) {
            latch.countDown()
        }
        return result
    }

    @KafkaListener(topics = ["\${kafka.topic}"], containerFactory = "kafkaContainerFactory")
    override fun consume2(
        @Payload payload: Dto,
        @Header(KafkaHeaders.RECEIVED_TOPIC) topic: String,
        @Header(KafkaHeaders.RECEIVED_TIMESTAMP) timestamp: Long,
        @Header(KafkaHeaders.OFFSET) offsets: Int,
        a: ConsumerRecord<String, Dto>,
        ack: Acknowledgment
    ): Boolean {
        val result = super.consume2(payload, topic, timestamp, offsets, a, ack)
        if ( result ) {
            latch.countDown()
        }
        return result
    }

    fun getLatch(): CountDownLatch {
        return latch
    }

}