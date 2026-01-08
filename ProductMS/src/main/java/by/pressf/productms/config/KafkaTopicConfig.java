package by.pressf.productms.config;

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
    NewTopic createProductDeadLetterTopic() {
        return TopicBuilder.name(env.getRequiredProperty("product.dlt.name"))
                .partitions(Integer.parseInt(env.getRequiredProperty("product.dlt.partitions")))
                .replicas(Integer.parseInt(env.getRequiredProperty("product.dlt.replicas")))
                .configs(Map.of("min.insync.replicas",
                        env.getRequiredProperty("product.dlt.min.insync.replicas")))
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
}
