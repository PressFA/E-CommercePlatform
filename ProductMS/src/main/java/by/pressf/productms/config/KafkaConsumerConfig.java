package by.pressf.productms.config;

import by.pressf.core.exceptions.EventException;
import by.pressf.core.exceptions.NotRetryableException;
import by.pressf.core.exceptions.RetryableException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class KafkaConsumerConfig {
    private final Environment env;

    @Bean
    ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                env.getRequiredProperty("spring.kafka.consumer.bootstrap-servers"));
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        config.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JacksonJsonDeserializer.class);
        config.put(JacksonJsonDeserializer.TRUSTED_PACKAGES,
                env.getRequiredProperty("spring.kafka.consumer.properties.spring.json.trusted.packages"));
        config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG,
                env.getRequiredProperty("spring.kafka.consumer.isolation-level"));

        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean // Бин-настройщик
    ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConsumerFactory<String, Object> consumerFactory,
            DeadLetterPublishingRecoverer recoverer
    ) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer,
                new FixedBackOff(3000, 3));

        errorHandler.addNotRetryableExceptions(NotRetryableException.class);
        errorHandler.addRetryableExceptions(RetryableException.class);
        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }

    /*
    Вторым параметром возвращаемого значения, передаётся лямбда-выражение, которое описывает
    логику обработки сообщения перед публикацией в dead letter topic (DLT)
    */
    @Bean
    DeadLetterPublishingRecoverer recoverer(KafkaTemplate<String, Object> kafkaTemplateDlt) {
        return new DeadLetterPublishingRecoverer(kafkaTemplateDlt,
                (consumerRecord, ex) -> {
                    if (ex.getCause() instanceof EventException e && e.getValue() != null) {
                        ProducerRecord<String, Object> record =
                                new ProducerRecord<>(e.getTopicName(), e.getKey(), e.getValue());
                        record.headers().add("messageId", e.getMessageId().getBytes());

                        try {
                            kafkaTemplateDlt.send(record).get();
                            log.info("Message {} successfully delivered to {} topic",
                                    e.getValue().getClass().getSimpleName(),
                                    e.getTopicName());
                        } catch (Exception sendEx) {
                            log.error("{}.class: {}", sendEx.getClass().getSimpleName(), sendEx.getMessage());
                            log.error("The message {} was not delivered to {} topic",
                                    e.getValue().getClass().getSimpleName(),
                                    e.getTopicName());
                        }
                    }

                    log.info("The message {} has been sent to {} topic",
                            consumerRecord.value().getClass().getSimpleName(),
                            env.getRequiredProperty("product.dlt.name"));
                    return new TopicPartition(
                            env.getRequiredProperty("product.dlt.name"),
                            consumerRecord.partition()
                    );
                }
        );
    }

    @Bean // Бин-kafkaTemplate для отправки сообщений в dead letter topic (DLT)
    KafkaTemplate<String, Object> kafkaTemplateDlt(ProducerFactory<String, Object> producerFactoryDlt) {
        return new KafkaTemplate<>(producerFactoryDlt);
    }

    @Bean // Бин-продюсер, который принимает мёртвые сообщения
    ProducerFactory<String, Object> producerFactoryDlt() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                env.getRequiredProperty("spring.kafka.producer.bootstrap-servers"));
        config.put(ProducerConfig.ACKS_CONFIG,
                env.getRequiredProperty("spring.kafka.producer.acks"));
        config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
                env.getRequiredProperty("spring.kafka.producer.properties.max.in.flight.requests.per.connection"));
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
                env.getRequiredProperty("spring.kafka.producer.properties.enable.idempotence"));
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);

        return new DefaultKafkaProducerFactory<>(config);
    }
}
