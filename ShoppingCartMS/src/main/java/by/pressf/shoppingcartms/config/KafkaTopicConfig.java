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
    NewTopic createCartCheckoutInitiatedTopic() {
        return TopicBuilder.name(env.getRequiredProperty("shopping-cart.events.topic.name"))
                .partitions(Integer.parseInt(env.getRequiredProperty("shopping-cart.events.topic.partitions")))
                .replicas(Integer.parseInt(env.getRequiredProperty("shopping-cart.events.topic.replicas")))
                .configs(Map.of("min.insync.replicas",
                        env.getRequiredProperty("shopping-cart.events.topic.min.insync.replicas")))
                .build();
    }
}
