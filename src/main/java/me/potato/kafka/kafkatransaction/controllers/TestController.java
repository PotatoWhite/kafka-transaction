package me.potato.kafka.kafkatransaction.controllers;

import lombok.extern.slf4j.Slf4j;
import me.potato.kafka.kafkatransaction.config.KafkaConfig;
import me.potato.kafka.kafkatransaction.models.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
public class TestController {

    public final KafkaTemplate kafkaTemplate;


    public TestController(KafkaTemplate kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }


    //http post 'localhost:8080/api/pub/a,b,c,d'
    @PostMapping("/api/pub/{cols}")
    public void sendCollection(@PathVariable String cols){
        kafkaTemplate.executeInTransaction(kafkaTemplate -> {
            StringUtils.commaDelimitedListToSet(cols).stream()
                    .map(item -> new Test(item))
                    .forEach(item -> kafkaTemplate.send( KafkaConfig.TOPIC01, item));

            return null;
        });
    }
}
