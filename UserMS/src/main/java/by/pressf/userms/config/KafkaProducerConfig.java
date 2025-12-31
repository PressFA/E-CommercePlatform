package by.pressf.userms.config;

import jakarta.persistence.EntityManagerFactory;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
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

    @Bean
    JpaTransactionManager jpaTransactionManager(EntityManagerFactory entityManagerFactory) {
        return new JpaTransactionManager(entityManagerFactory);
    }

    @Bean
    NewTopic createUserEventsTopic() {
        return TopicBuilder.name(env.getRequiredProperty("user.events.topic.name"))
                .partitions(Integer.parseInt(env.getRequiredProperty("user.events.topic.partitions")))
                .replicas(Integer.parseInt(env.getRequiredProperty("user.events.topic.replicas")))
                .configs(Map.of("min.insync.replicas",
                        env.getRequiredProperty("user.events.topic.min.insync.replicas")))
                .build();
    }
}
