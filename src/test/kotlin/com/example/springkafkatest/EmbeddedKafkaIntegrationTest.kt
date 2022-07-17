package com.example.springkafkatest

import com.example.springkafkatest.consumer.KafkaConsumerForTest
import com.example.springkafkatest.dto.Dto
import com.example.springkafkatest.producer.KafkaProducer
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.support.SendResult
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import org.springframework.util.concurrent.FailureCallback
import org.springframework.util.concurrent.SuccessCallback
import java.time.LocalDateTime
import java.time.ZoneId


@SpringBootTest
@DirtiesContext
@EmbeddedKafka(
    partitions = 1,
    brokerProperties = [
        "listeners=PLAINTEXT://localhost:9092",
        "port=9092"
    ]
)
@ActiveProfiles("test")
class EmbeddedKafkaIntegrationTest {

    @Autowired
    lateinit var consumer: KafkaConsumerForTest

    @Autowired
    lateinit var producer: KafkaProducer

    @Value("\${kafka.topic}")
    lateinit var topic: String

    @Test
    fun givenEmbeddedKafkaBroker_whenSendingtoSimpleProducer_thenMessageReceived() {
        Thread.sleep(1000)

        val oneHourAgoPlusSomeSeconds = LocalDateTime.now().minusHours(1).plusSeconds(10)

        for (i in 1 .. 10) {
            val result = producer.send(topic, Dto(
                "send $i",
                oneHourAgoPlusSomeSeconds.plusSeconds(i.toLong()).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()
            ))
            println(result.get())
        }
        Thread.sleep(1000 * 10)

        for (i in 11 .. 13) {
            val result = producer.send(topic, Dto(
                "send $i",
                oneHourAgoPlusSomeSeconds.plusSeconds(i.toLong()).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()
            ))
            println(result.get())
        }

        Thread.sleep(1000 * 1000000)

    }

    @Test
    fun givenEmbeddedKafkaBroker_whenSendingtoSimpleProducer_thenMessageReceived2() {
        Thread.sleep(1000)

        val successCallback = SuccessCallback<SendResult<String, Dto>>() {
            it?.let {
                println("sendResult: $it")
            }
        }
        val failureCallback = FailureCallback() {
            println("failed: ${it.message}")
        }

        producer.send(
            topic,
            Dto(
                "send data",
                LocalDateTime.now().minusHours(1).plusSeconds(10).plusSeconds(2).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()
            ),
            failureCallback,
            successCallback
        )

        consumer.getLatch().await()

        println("done")
        Thread.sleep(1000 * 1000000)

    }
}