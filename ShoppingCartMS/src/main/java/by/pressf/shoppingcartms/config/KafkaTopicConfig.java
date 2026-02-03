package by.pressf.shoppingcartms.config;

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
    NewTopic createROrderWCartEventsTopic() {
        return TopicBuilder.name(env.getRequiredProperty("r-order-w-cart.topic.name"))
                .partitions(Integer.parseInt(env.getRequiredProperty("r-order-w-cart.topic.partitions")))
                .replicas(Integer.parseInt(env.getRequiredProperty("r-order-w-cart.topic.replicas")))
                .configs(Map.of("min.insync.replicas",
                        env.getRequiredProperty("r-order-w-cart.topic.min.insync.replicas")))
                .build();
    }
}
