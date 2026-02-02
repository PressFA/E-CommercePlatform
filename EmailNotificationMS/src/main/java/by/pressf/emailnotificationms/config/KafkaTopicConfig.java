package by.pressf.emailnotificationms.config;

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
    NewTopic createSendNotificationEventTopic() {
        return TopicBuilder.name(env.getRequiredProperty("email-notification.events.topic.name"))
                .partitions(Integer.parseInt(env.getRequiredProperty("email-notification.events.topic.partitions")))
                .replicas(Integer.parseInt(env.getRequiredProperty("email-notification.events.topic.replicas")))
                .configs(Map.of("min.insync.replicas",
                        env.getRequiredProperty("email-notification.events.topic.min.insync.replicas")))
                .build();
    }

    @Bean
    NewTopic createEmailNotificationCommandsTopic() {
        return TopicBuilder.name(env.getRequiredProperty("email-notification.commands.topic.name"))
                .partitions(Integer.parseInt(env.getRequiredProperty("email-notification.commands.topic.partitions")))
                .replicas(Integer.parseInt(env.getRequiredProperty("email-notification.commands.topic.replicas")))
                .configs(Map.of("min.insync.replicas",
                        env.getRequiredProperty("email-notification.commands.topic.min.insync.replicas")))
                .build();
    }

    @Bean
    NewTopic createEmailNotificationDeadLetterTopic() {
        return TopicBuilder.name(env.getRequiredProperty("email-notification.dlt.name"))
                .partitions(Integer.parseInt(env.getRequiredProperty("email-notification.dlt.partitions")))
                .replicas(Integer.parseInt(env.getRequiredProperty("email-notification.dlt.replicas")))
                .configs(Map.of("min.insync.replicas",
                        env.getRequiredProperty("email-notification.dlt.min.insync.replicas")))
                .build();
    }

    @Bean
    NewTopic createUserEmailEventsTopic() {
        return TopicBuilder.name(env.getRequiredProperty("user-email.events.topic.name"))
                .partitions(Integer.parseInt(env.getRequiredProperty("user-email.events.topic.partitions")))
                .replicas(Integer.parseInt(env.getRequiredProperty("user-email.events.topic.replicas")))
                .configs(Map.of("min.insync.replicas",
                        env.getRequiredProperty("user-email.events.topic.min.insync.replicas")))
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
