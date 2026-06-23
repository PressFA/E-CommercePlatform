package by.pressf.userms.it.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

import java.util.HashMap;
import java.util.Map;

@TestConfiguration
public class TestProducerConfig {
    @Bean
    DefaultKafkaProducerFactory<String, Object> testProducerFactory(Environment env) {
        Map<String, Object> producerConfig = new HashMap<>();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                env.getRequiredProperty("spring.kafka.producer.bootstrap-servers"));
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);

        return new DefaultKafkaProducerFactory<>(producerConfig);
    }

    @Bean
    KafkaTemplate<String, Object> externalProducer(DefaultKafkaProducerFactory<String, Object> testProducerFactory) {
        return new KafkaTemplate<>(testProducerFactory);
    }
}
