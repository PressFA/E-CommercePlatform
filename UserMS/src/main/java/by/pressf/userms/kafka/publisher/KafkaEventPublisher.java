package by.pressf.userms.kafka.publisher;

import by.pressf.core.dto.choreography.events.BalanceTopUpFailedEvent;
import by.pressf.core.dto.choreography.events.UserBalanceCreditedEvent;
import by.pressf.core.dto.orchestration.events.user.UserBalanceDebitCanceledEvent;
import by.pressf.core.dto.orchestration.events.user.UserBalanceDebitedEvent;
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

    public void sendUserBalanceCreditedEvent(String keyMessage, UserBalanceCreditedEvent valueMessage) {
        send(env.getRequiredProperty("r-payment-w-user.topic.name"), keyMessage, valueMessage);
    }

    public void sendMessageUserBalanceDebitedEvent(String keyMessage, UserBalanceDebitedEvent valueMessage) {
        send(env.getRequiredProperty("successful-events.topic.name"), keyMessage, valueMessage);
    }

    public void sendMessageUserBalanceDebitCanceledEvent(String keyMessage, UserBalanceDebitCanceledEvent valueMessage) {
        send(env.getRequiredProperty("compensating-events.topic.name"), keyMessage, valueMessage);
    }

    public void sendMessageBalanceTopUpFailedEvent(String keyMessage, BalanceTopUpFailedEvent valueMessage) {
        send(env.getRequiredProperty("r-email-w-user.topic.name"), keyMessage, valueMessage);
    }

    private void send(String topic, String key, Object value) {
        ProducerRecord<String, Object> record = new ProducerRecord<>(topic, key, value);
        record.headers().add("messageId", UUID.randomUUID().toString().getBytes());

        kafkaTemplate.send(record);
        log.info("The {} message was sent to the {} topic.", value.getClass().getSimpleName(), topic);
    }
}
