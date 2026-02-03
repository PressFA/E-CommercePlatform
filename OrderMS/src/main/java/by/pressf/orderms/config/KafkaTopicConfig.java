package by.pressf.orderms.config;

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
    NewTopic createOrderCommandsTopic() {
        return TopicBuilder.name(env.getRequiredProperty("order.commands.topic.name"))
                .partitions(Integer.parseInt(env.getRequiredProperty("order.commands.topic.partitions")))
                .replicas(Integer.parseInt(env.getRequiredProperty("order.commands.topic.replicas")))
                .configs(Map.of("min.insync.replicas",
                        env.getRequiredProperty("order.commands.topic.min.insync.replicas")))
                .build();
    }

    @Bean
    NewTopic createOrderDeadLetterTopic() {
        return TopicBuilder.name(env.getRequiredProperty("order.dlt.name"))
                .partitions(Integer.parseInt(env.getRequiredProperty("order.dlt.partitions")))
                .replicas(Integer.parseInt(env.getRequiredProperty("order.dlt.replicas")))
                .configs(Map.of("min.insync.replicas",
                        env.getRequiredProperty("order.dlt.min.insync.replicas")))
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
    NewTopic createROrderWCartEventsTopic() {
        return TopicBuilder.name(env.getRequiredProperty("r-order-w-cart.topic.name"))
                .partitions(Integer.parseInt(env.getRequiredProperty("r-order-w-cart.topic.partitions")))
                .replicas(Integer.parseInt(env.getRequiredProperty("r-order-w-cart.topic.replicas")))
                .configs(Map.of("min.insync.replicas",
                        env.getRequiredProperty("r-order-w-cart.topic.min.insync.replicas")))
                .build();
    }
}
