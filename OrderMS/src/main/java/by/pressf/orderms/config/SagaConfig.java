package by.pressf.orderms.config;

import by.pressf.core.exceptions.NotRetryableException;
import by.pressf.core.exceptions.RetryableException;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Configuration
@RequiredArgsConstructor
public class SagaConfig {
    private final Environment env;

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, Object> sagaKafkaListenerContainerFactory(
            ConsumerFactory<String, Object> consumerFactory,
            KafkaTemplate<String, Object> sagaKafkaTemplateDlt) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);

        DeadLetterPublishingRecoverer recoverer =
                new DeadLetterPublishingRecoverer(
                        sagaKafkaTemplateDlt,
                        (record, ex) ->
                                new TopicPartition(env.getRequiredProperty("saga.dlt.name"), record.partition())
                );

        DefaultErrorHandler errorHandler =
                new DefaultErrorHandler(recoverer, new FixedBackOff(3000, 3));

        errorHandler.addNotRetryableExceptions(NotRetryableException.class);
        errorHandler.addRetryableExceptions(RetryableException.class);
        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }

    @Bean
    KafkaTemplate<String, Object> sagaKafkaTemplateDlt(ProducerFactory<String, Object> sagaProducerFactoryDlt) {
        return new KafkaTemplate<>(sagaProducerFactoryDlt);
    }

    @Bean
    ProducerFactory<String, Object> sagaProducerFactoryDlt() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                env.getRequiredProperty("spring.kafka.consumer.bootstrap-servers"));
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);
        config.put(ProducerConfig.RETRIES_CONFIG,
                env.getRequiredProperty("spring.kafka.producer.retries"));
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,
                env.getRequiredProperty("spring.kafka.producer.properties.delivery.timeout.ms"));
        config.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG,
                env.getRequiredProperty("spring.kafka.producer.properties.retry.backoff.ms"));
        config.put(ProducerConfig.LINGER_MS_CONFIG,
                env.getRequiredProperty("spring.kafka.producer.properties.linger.ms"));
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,
                env.getRequiredProperty("spring.kafka.producer.properties.request.timeout.ms"));
        config.put(ProducerConfig.ACKS_CONFIG,
                env.getRequiredProperty("spring.kafka.producer.acks"));
        config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
                env.getRequiredProperty("spring.kafka.producer.properties.max.in.flight.requests.per.connection"));
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
                env.getRequiredProperty("spring.kafka.producer.properties.enable.idempotence"));
        config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,
                env.getRequiredProperty("saga.dlt.transaction-id-prefix"));

        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    NewTopic createOrderCommandsTopic() {
        return TopicBuilder.name(env.getRequiredProperty("order.commands.topic.name"))
                .partitions(Integer.parseInt(env.getRequiredProperty("order.commands.topic.partitions")))
                .replicas(Integer.parseInt(env.getRequiredProperty("order.commands.topic.replicas")))
                .configs(Map.of("min.insync.replicas",
                        env.getRequiredProperty("order.commands.topic.min.insync.replicas")))
                .build();
    }

    @Bean
    NewTopic createProductCommandsTopic() {
        return TopicBuilder.name(env.getRequiredProperty("product.commands.topic.name"))
                .partitions(Integer.parseInt(env.getRequiredProperty("product.commands.topic.partitions")))
                .replicas(Integer.parseInt(env.getRequiredProperty("product.commands.topic.replicas")))
                .configs(Map.of("min.insync.replicas",
                        env.getRequiredProperty("product.commands.topic.min.insync.replicas")))
                .build();
    }

    @Bean
    NewTopic createPaymentCommandsTopic() {
        return TopicBuilder.name(env.getRequiredProperty("payment.commands.topic.name"))
                .partitions(Integer.parseInt(env.getRequiredProperty("payment.commands.topic.partitions")))
                .replicas(Integer.parseInt(env.getRequiredProperty("payment.commands.topic.replicas")))
                .configs(Map.of("min.insync.replicas",
                        env.getRequiredProperty("payment.commands.topic.min.insync.replicas")))
                .build();
    }

    @Bean
    NewTopic createUserCommandsTopic() {
        return TopicBuilder.name(env.getRequiredProperty("user.commands.topic.name"))
                .partitions(Integer.parseInt(env.getRequiredProperty("user.commands.topic.partitions")))
                .replicas(Integer.parseInt(env.getRequiredProperty("user.commands.topic.replicas")))
                .configs(Map.of("min.insync.replicas",
                        env.getRequiredProperty("user.commands.topic.min.insync.replicas")))
                .build();
    }
}
