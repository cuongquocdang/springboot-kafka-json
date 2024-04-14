package com.example.kafkaproducer;

import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/v1/simulators")
@RequiredArgsConstructor
public class KafkaSimulator {

    private final KafkaProducer kafkaProducer;

    @PostMapping("/single")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void send(@RequestBody final User user) {
        kafkaProducer.produce(user);
    }
}