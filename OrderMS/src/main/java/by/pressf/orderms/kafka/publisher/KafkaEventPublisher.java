package by.pressf.orderms.kafka.publisher;

import by.pressf.core.dto.orchestration.events.order.OrderCompletedEvent;
import by.pressf.core.dto.orchestration.events.order.OrderCreatedEvent;
import by.pressf.core.dto.orchestration.events.order.OrderRejectedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jspecify.annotations.NonNull;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Objects;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaEventPublisher {
    private final Environment env;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void sendOrderCreatedEvent(@NonNull String keyMessage,
                                      @NonNull OrderCreatedEvent valueMessage) {
        Objects.requireNonNull(keyMessage);
        Objects.requireNonNull(valueMessage);

        send(env.getRequiredProperty("successful-events.topic.name"), keyMessage, valueMessage);
    }

    public void sendOrderCompletedEvent(@NonNull String keyMessage,
                                        @NonNull OrderCompletedEvent valueMessage) {
        Objects.requireNonNull(keyMessage);
        Objects.requireNonNull(valueMessage);

        send(env.getRequiredProperty("successful-events.topic.name"), keyMessage, valueMessage);
    }

    public void sendOrderRejectedEvent(@NonNull String keyMessage,
                                       @NonNull OrderRejectedEvent valueMessage) {
        Objects.requireNonNull(keyMessage);
        Objects.requireNonNull(valueMessage);

        send(env.getRequiredProperty("compensating-events.topic.name"), keyMessage, valueMessage);
    }

    private void send(String topic, String key, Object value) {
        ProducerRecord<String, Object> record = new ProducerRecord<>(topic, key, value);
        record.headers().add("messageId", UUID.randomUUID().toString().getBytes());

        kafkaTemplate.send(record);
        log.info("The {} message was sent to the {} topic.", value.getClass().getSimpleName(), topic);
    }
}
