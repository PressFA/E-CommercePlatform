package by.pressf.productms.it.config;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@TestConfiguration
public class TestProducerAndConsumerConfig {
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

    @Bean
    DefaultKafkaConsumerFactory<String, String> testConsumerFactory(Environment env) {
        Map<String, Object> consumerConfig  = new HashMap<>();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                env.getRequiredProperty("spring.kafka.producer.bootstrap-servers"));
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-" + System.nanoTime());
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        return new DefaultKafkaConsumerFactory<>(consumerConfig);
    }

    @Bean
    Consumer<String, String> spyConsumer(DefaultKafkaConsumerFactory<String, String> testConsumerFactory,
                                         Environment env) {
        Consumer<String, String> consumer = testConsumerFactory.createConsumer();
        consumer.subscribe(List.of(
                env.getRequiredProperty("successful-events.topic.name"),
                env.getRequiredProperty("errors-successful-events.topic.name"),
                env.getRequiredProperty("compensating-events.topic.name"),
                env.getRequiredProperty("errors-compensating-events.topic.name")
        ));

        return consumer;
    }
}
