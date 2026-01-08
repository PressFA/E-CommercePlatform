package by.pressf.orderms.config;

import jakarta.persistence.EntityManagerFactory;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.orm.jpa.JpaTransactionManager;

import java.util.HashMap;
import java.util.Map;

@Configuration
@RequiredArgsConstructor
public class KafkaProducerConfig {
    private final Environment env;

    @Bean
    ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                env.getRequiredProperty("spring.kafka.producer.bootstrap-servers"));
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG,
                env.getRequiredProperty("spring.kafka.producer.acks"));
        config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
                env.getRequiredProperty("spring.kafka.producer.properties.max.in.flight.requests.per.connection"));
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
                env.getRequiredProperty("spring.kafka.producer.properties.enable.idempotence"));
        config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,
                env.getRequiredProperty("spring.kafka.producer.transaction-id-prefix"));
        config.put(ProducerConfig.RETRIES_CONFIG,
                env.getRequiredProperty("spring.kafka.producer.retries"));
        config.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG,
                env.getRequiredProperty("spring.kafka.producer.properties.retry.backoff.ms"));
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,
                env.getRequiredProperty("spring.kafka.producer.properties.delivery.timeout.ms"));
        config.put(ProducerConfig.LINGER_MS_CONFIG,
                env.getRequiredProperty("spring.kafka.producer.properties.linger.ms"));
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,
                env.getRequiredProperty("spring.kafka.producer.properties.request.timeout.ms"));

        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    KafkaTransactionManager<String, Object> kafkaTransactionManager(ProducerFactory<String, Object> producerFactory) {
        return new KafkaTransactionManager<>(producerFactory);
    }

    @Bean("transactionManager")
    JpaTransactionManager jpaTransactionManager(EntityManagerFactory entityManagerFactory) {
        return new JpaTransactionManager(entityManagerFactory);
    }
}
