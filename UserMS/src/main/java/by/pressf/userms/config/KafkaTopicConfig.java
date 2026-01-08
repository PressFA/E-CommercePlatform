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

    @Bean
    NewTopic createUserDeadLetterTopic() {
        return TopicBuilder.name(env.getRequiredProperty("user.dlt.name"))
                .partitions(Integer.parseInt(env.getRequiredProperty("user.dlt.partitions")))
                .replicas(Integer.parseInt(env.getRequiredProperty("user.dlt.replicas")))
                .configs(Map.of("min.insync.replicas",
                        env.getRequiredProperty("user.dlt.min.insync.replicas")))
                .build();
    }
}
