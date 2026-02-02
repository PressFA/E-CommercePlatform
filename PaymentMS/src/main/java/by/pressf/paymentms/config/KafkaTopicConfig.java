package by.pressf.paymentms.config;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.kafka.config.TopicBuilder;

import java.util.Map;

@Configuration
@RequiredArgsConstructor
public class KafkaTopicConfig {
    private final Environment env;

    @Bean
    NewTopic createPaymentEventsTopic() {
        return TopicBuilder.name(env.getRequiredProperty("payment.events.topic.name"))
                .partitions(Integer.parseInt(env.getRequiredProperty("payment.events.topic.partitions")))
                .replicas(Integer.parseInt(env.getRequiredProperty("payment.events.topic.replicas")))
                .configs(Map.of("min.insync.replicas",
                        env.getRequiredProperty("payment.events.topic.min.insync.replicas")))
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
    NewTopic createPaymentDeadLetterTopic() {
        return TopicBuilder.name(env.getRequiredProperty("payment.dlt.name"))
                .partitions(Integer.parseInt(env.getRequiredProperty("payment.dlt.partitions")))
                .replicas(Integer.parseInt(env.getRequiredProperty("payment.dlt.replicas")))
                .configs(Map.of("min.insync.replicas",
                        env.getRequiredProperty("payment.dlt.min.insync.replicas")))
                .build();
    }

    @Bean
    NewTopic createUserPaymentEventsTopic() {
        return TopicBuilder.name(env.getRequiredProperty("user-payment.events.topic.name"))
                .partitions(Integer.parseInt(env.getRequiredProperty("user-payment.events.topic.partitions")))
                .replicas(Integer.parseInt(env.getRequiredProperty("user-payment.events.topic.replicas")))
                .configs(Map.of("min.insync.replicas",
                        env.getRequiredProperty("user-payment.events.topic.min.insync.replicas")))
                .build();
    }

    @Bean
    NewTopic createEmailPaymentEventsTopic() {
        return TopicBuilder.name(env.getRequiredProperty("email-payment.events.topic.name"))
                .partitions(Integer.parseInt(env.getRequiredProperty("email-payment.events.topic.partitions")))
                .replicas(Integer.parseInt(env.getRequiredProperty("email-payment.events.topic.replicas")))
                .configs(Map.of("min.insync.replicas",
                        env.getRequiredProperty("email-payment.events.topic.min.insync.replicas")))
                .build();
    }
}
