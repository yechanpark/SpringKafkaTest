package com.example.springkafkatest

import com.example.springkafkatest.consumer.KafkaConsumer
import com.example.springkafkatest.dto.Dto
import com.example.springkafkatest.producer.KafkaProducer
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.annotation.DirtiesContext

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(
    partitions = 1,
    brokerProperties = [
        "listeners=PLAINTEXT://localhost:9092",
        "port=9092"
    ]
)
class EmbeddedKafkaIntegrationTest {

    @Autowired
    lateinit var consumer: KafkaConsumer

    @Autowired
    lateinit var producer: KafkaProducer

    @Value("\${kafka.topic}")
    lateinit var topic: String

    @Test
    @Throws(Exception::class)
    fun givenEmbeddedKafkaBroker_whenSendingtoSimpleProducer_thenMessageReceived() {
        Thread.sleep(1000)
        for (i in 1 .. 10) {
            producer.send(topic, Dto("send $i"))
        }
        Thread.sleep(1000 * 5)
        for (i in 11 .. 13) {
            producer.send(topic, Dto("send $i"))
        }

        Thread.sleep(1000 * 1000000)

    }
}