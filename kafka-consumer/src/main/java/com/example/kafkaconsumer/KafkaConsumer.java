package com.example.kafkaconsumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class KafkaConsumer {

    private final ObjectMapper objectMapper;

    private static final String PRODUCE_TOPIC = "USER-TOPIC";

    @KafkaListener(topics = PRODUCE_TOPIC)
    public void consume(String json) {

        var user = parseToObject(json);
        log.info("Consumed message successfully: {}", user);
    }

    @SneakyThrows
    private User parseToObject(final String json) {
        return objectMapper.readValue(json, User.class);
    }
}
