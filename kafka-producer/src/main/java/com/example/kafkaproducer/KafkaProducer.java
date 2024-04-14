package com.example.kafkaproducer;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class KafkaProducer {

    private final ObjectMapper objectMapper;
    private final KafkaTemplate<String, String> kafkaTemplate;

    private static final String PRODUCE_TOPIC = "USER-TOPIC";

    public void produce(final User user) {

        var parsedJson = parseToJsonString(user);

        var resultFuture = kafkaTemplate.send(PRODUCE_TOPIC, parsedJson);
        // add callback
        resultFuture.whenComplete((result, throwable) -> {
            if (throwable == null) {
                logSuccessProducer(result.getRecordMetadata());
            } else {
                logErrorProducer(throwable);
            }
        });
    }

    @SneakyThrows
    private String parseToJsonString(final User user) {
        return objectMapper.writeValueAsString(user);
    }

    private static void logSuccessProducer(final RecordMetadata recordMetadata) {
        log.info("[KAFKA-PRODUCER] Produced message successfully to topic={}, partition={}, offset={}",
                PRODUCE_TOPIC, recordMetadata.partition(), recordMetadata.offset());
    }

    private static void logErrorProducer(final Throwable throwable) {
        log.error("[KAFKA-PRODUCER] Produce message failed", throwable);
    }
}