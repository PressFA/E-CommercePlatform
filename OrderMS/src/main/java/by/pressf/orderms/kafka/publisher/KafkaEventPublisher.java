package by.pressf.orderms.kafka.publisher;

import by.pressf.core.dto.orchestration.events.order.OrderCompletedEvent;
import by.pressf.core.dto.orchestration.events.order.OrderCreatedEvent;
import by.pressf.core.dto.orchestration.events.order.OrderRejectedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jspecify.annotations.NullMarked;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Slf4j
@Component
@NullMarked
@RequiredArgsConstructor
public class KafkaEventPublisher {
    private final Environment env;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void sendOrderCreatedEvent(String keyMessage, OrderCreatedEvent valueMessage) {
        send(env.getRequiredProperty("successful-events.topic.name"), keyMessage, valueMessage);
    }

    public void sendOrderCompletedEvent(String keyMessage, OrderCompletedEvent valueMessage) {
        send(env.getRequiredProperty("successful-events.topic.name"), keyMessage, valueMessage);
    }

    public void sendOrderRejectedEvent(String keyMessage, OrderRejectedEvent valueMessage) {
        send(env.getRequiredProperty("compensating-events.topic.name"), keyMessage, valueMessage);
    }

    private void send(String topic, String key, Object value) {
        ProducerRecord<String, Object> record = new ProducerRecord<>(topic, key, value);
        record.headers().add("messageId", UUID.randomUUID().toString().getBytes());

        kafkaTemplate.send(record);
        log.info("The {} message was sent to the {} topic.", value.getClass().getSimpleName(), topic);
    }
}
