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
    NewTopic createOrderEventsTopic() {
        return TopicBuilder.name(env.getRequiredProperty("order.events.topic.name"))
                .partitions(Integer.parseInt(env.getRequiredProperty("order.events.topic.partitions")))
                .replicas(Integer.parseInt(env.getRequiredProperty("order.events.topic.replicas")))
                .configs(Map.of("min.insync.replicas",
                        env.getRequiredProperty("order.events.topic.min.insync.replicas")))
                .build();
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
    NewTopic createOrderDeadLetterTopic() {
        return TopicBuilder.name(env.getRequiredProperty("order.dlt.name"))
                .partitions(Integer.parseInt(env.getRequiredProperty("order.dlt.partitions")))
                .replicas(Integer.parseInt(env.getRequiredProperty("order.dlt.replicas")))
                .configs(Map.of("min.insync.replicas",
                        env.getRequiredProperty("order.dlt.min.insync.replicas")))
                .build();
    }

    @Bean
    NewTopic createProductEventsTopic() {
        return TopicBuilder.name(env.getRequiredProperty("product.events.topic.name"))
                .partitions(Integer.parseInt(env.getRequiredProperty("product.events.topic.partitions")))
                .replicas(Integer.parseInt(env.getRequiredProperty("product.events.topic.replicas")))
                .configs(Map.of("min.insync.replicas",
                        env.getRequiredProperty("product.events.topic.min.insync.replicas")))
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
    NewTopic createUserEventsTopic() {
        return TopicBuilder.name(env.getRequiredProperty("user.events.topic.name"))
                .partitions(Integer.parseInt(env.getRequiredProperty("user.events.topic.partitions")))
                .replicas(Integer.parseInt(env.getRequiredProperty("user.events.topic.replicas")))
                .configs(Map.of("min.insync.replicas",
                        env.getRequiredProperty("user.events.topic.min.insync.replicas")))
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
