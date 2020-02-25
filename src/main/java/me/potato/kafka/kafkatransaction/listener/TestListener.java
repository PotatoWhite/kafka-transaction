package me.potato.kafka.kafkatransaction.listener;

import lombok.extern.slf4j.Slf4j;
import me.potato.kafka.kafkatransaction.config.KafkaConfig;
import me.potato.kafka.kafkatransaction.models.Test;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;

@Slf4j
@Component
public class TestListener {

    private final KafkaTemplate kafkaTemplate;

    public TestListener(KafkaTemplate kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }


    @KafkaListener(id = "test-group", topics = KafkaConfig.TOPIC01)
    public void listen(List<Test> foos) throws IOException {
        log.info("Received(LIST): " + foos);
        foos.forEach(f -> kafkaTemplate.send(KafkaConfig.TOPIC02, f.getData().toUpperCase()));
        log.info("Messages sent, hit enter to commit tx");

    }

    @KafkaListener(id = "test-group-02", topics = KafkaConfig.TOPIC02)
    public void listen(String in) {
        log.info("Received: " + in);
    }
}
