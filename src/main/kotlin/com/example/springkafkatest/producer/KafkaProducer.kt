package com.example.springkafkatest.producer

import com.example.springkafkatest.dto.Dto
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.SendResult
import org.springframework.stereotype.Component
import org.springframework.util.concurrent.FailureCallback
import org.springframework.util.concurrent.ListenableFuture
import org.springframework.util.concurrent.SuccessCallback

@Component
class KafkaProducer(
    private val kafkaTemplate: KafkaTemplate<String, Dto>
) {
    fun send(topic: String, payload: Dto): ListenableFuture<SendResult<String, Dto>> {
        println("KafkaProducer send(topic: $topic, payload: $payload")
        return kafkaTemplate.send(topic, payload)
    }

    fun send(
        topic: String,
        payload: Dto,
        failCallback: FailureCallback,
        successCallback: SuccessCallback<SendResult<String, Dto>>
    ): ListenableFuture<SendResult<String, Dto>> {
        println("KafkaProducer send(topic: $topic, payload: $payload")
        val result = kafkaTemplate.send(topic, payload)
        result.addCallback(successCallback, failCallback)
        return result
    }
}