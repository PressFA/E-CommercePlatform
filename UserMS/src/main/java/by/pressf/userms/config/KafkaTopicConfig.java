package by.pressf.userms.config;

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
    NewTopic createUserCommandsTopic() {
        return TopicBuilder.name(env.getRequiredProperty("user.commands.topic.name"))
                .partitions(Integer.parseInt(env.getRequiredProperty("user.commands.topic.partitions")))
                .replicas(Integer.parseInt(env.getRequiredProperty("user.commands.topic.replicas")))
                .configs(Map.of("min.insync.replicas",
                        env.getRequiredProperty("user.commands.topic.min.insync.replicas")))
                .build();
    }

    @Bean
    NewTopic createUserDeadLetterTopic() {
        return TopicBuilder.name(env.getRequiredProperty("user.dlt.name"))
                .partitions(Integer.parseInt(env.getRequiredProperty("user.dlt.partitions")))
                .replicas(Integer.parseInt(env.getRequiredProperty("user.dlt.replicas")))
                .configs(Map.of("min.insync.replicas",
                        env.getRequiredProperty("user.dlt.min.insync.replicas")))
                .build();
    }

    @Bean
    NewTopic createSuccessfulEventsTopic() {
        return TopicBuilder.name(env.getRequiredProperty("successful-events.topic.name"))
                .partitions(Integer.parseInt(env.getRequiredProperty("successful-events.topic.partitions")))
                .replicas(Integer.parseInt(env.getRequiredProperty("successful-events.topic.replicas")))
                .configs(Map.of("min.insync.replicas",
                        env.getRequiredProperty("successful-events.topic.min.insync.replicas")))
                .build();
    }

    @Bean
    NewTopic createErrorsSuccessfulEventsTopic() {
        return TopicBuilder.name(env.getRequiredProperty("errors-successful-events.topic.name"))
                .partitions(Integer.parseInt(env.getRequiredProperty("errors-successful-events.topic.partitions")))
                .replicas(Integer.parseInt(env.getRequiredProperty("errors-successful-events.topic.replicas")))
                .configs(Map.of("min.insync.replicas",
                        env.getRequiredProperty("errors-successful-events.topic.min.insync.replicas")))
                .build();
    }

    @Bean
    NewTopic createCompensatingEventsTopic() {
        return TopicBuilder.name(env.getRequiredProperty("compensating-events.topic.name"))
                .partitions(Integer.parseInt(env.getRequiredProperty("compensating-events.topic.partitions")))
                .replicas(Integer.parseInt(env.getRequiredProperty("compensating-events.topic.replicas")))
                .configs(Map.of("min.insync.replicas",
                        env.getRequiredProperty("compensating-events.topic.min.insync.replicas")))
                .build();
    }

    @Bean
    NewTopic createErrorsCompensatingEventsTopic() {
        return TopicBuilder.name(env.getRequiredProperty("errors-compensating-events.topic.name"))
                .partitions(Integer.parseInt(env.getRequiredProperty("errors-compensating-events.topic.partitions")))
                .replicas(Integer.parseInt(env.getRequiredProperty("errors-compensating-events.topic.replicas")))
                .configs(Map.of("min.insync.replicas",
                        env.getRequiredProperty("errors-compensating-events.topic.min.insync.replicas")))
                .build();
    }

    @Bean
    NewTopic createRUserWPaymentEventsTopic() {
        return TopicBuilder.name(env.getRequiredProperty("r-user-w-payment.topic.name"))
                .partitions(Integer.parseInt(env.getRequiredProperty("r-user-w-payment.topic.partitions")))
                .replicas(Integer.parseInt(env.getRequiredProperty("r-user-w-payment.topic.replicas")))
                .configs(Map.of("min.insync.replicas",
                        env.getRequiredProperty("r-user-w-payment.topic.min.insync.replicas")))
                .build();
    }

    @Bean
    NewTopic createRPaymentWUserEventsTopic() {
        return TopicBuilder.name(env.getRequiredProperty("r-payment-w-user.topic.name"))
                .partitions(Integer.parseInt(env.getRequiredProperty("r-payment-w-user.topic.partitions")))
                .replicas(Integer.parseInt(env.getRequiredProperty("r-payment-w-user.topic.replicas")))
                .configs(Map.of("min.insync.replicas",
                        env.getRequiredProperty("r-payment-w-user.topic.min.insync.replicas")))
                .build();
    }

    @Bean
    NewTopic createREmailWUserEventsTopic() {
        return TopicBuilder.name(env.getRequiredProperty("r-email-w-user.topic.name"))
                .partitions(Integer.parseInt(env.getRequiredProperty("r-email-w-user.topic.partitions")))
                .replicas(Integer.parseInt(env.getRequiredProperty("r-email-w-user.topic.replicas")))
                .configs(Map.of("min.insync.replicas",
                        env.getRequiredProperty("r-email-w-user.topic.min.insync.replicas")))
                .build();
    }
}
