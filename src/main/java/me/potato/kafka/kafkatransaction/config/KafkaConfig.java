package me.potato.kafka.kafkatransaction.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.converter.BatchMessagingMessageConverter;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;

@Slf4j
@Configuration
public class KafkaConfig {
    public static final String TOPIC01 = "testTopic01";
    public static final String TOPIC02 = "testTopic02";

    @Bean
    public NewTopic createTestTopic01() {
        return new NewTopic(TOPIC01, 1, (short) 1);
    }

    @Bean
    public NewTopic createTestTopic02() {
        return new NewTopic(TOPIC02, 1, (short) 1);
    }


    @Bean
    public ConcurrentKafkaListenerContainerFactory kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory consumerFactory,
            KafkaTemplate kafkaTemplate) {

        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, consumerFactory);
        factory.setBatchListener(true); // mand.
        factory.setMessageConverter(batchConverter());
        return factory;
    }

    @Bean
    public BatchMessagingMessageConverter batchConverter() {
        return new BatchMessagingMessageConverter(converter());
    }

    @Bean
    public RecordMessageConverter converter() {
        return new StringJsonMessageConverter();
    }
}
