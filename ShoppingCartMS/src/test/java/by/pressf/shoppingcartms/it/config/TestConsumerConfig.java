package by.pressf.shoppingcartms.it.config;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@TestConfiguration
public class TestConsumerConfig {
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
        consumer.subscribe(Collections.singleton(env.getRequiredProperty("r-order-w-cart.topic.name")));

        return consumer;
    }
}
