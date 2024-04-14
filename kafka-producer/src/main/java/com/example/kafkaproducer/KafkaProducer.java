package com.example.kafkaproducer;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class KafkaProducer {

    private final ObjectMapper objectMapper;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public void produce(final User user) {

        var parsedJson = parseToJsonString(user);

        kafkaTemplate.send("USER-TOPIC", parsedJson);
    }

    @SneakyThrows
    private String parseToJsonString(final User user) {
        return objectMapper.writeValueAsString(user);
    }
}