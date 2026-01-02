package by.pressf.orderms.service.handler;

import by.pressf.core.dto.commands.ConfirmOrderCommand;
import by.pressf.core.dto.events.OrderCompletionFailedEvent;
import by.pressf.orderms.dao.entity.EventEntity;
import by.pressf.orderms.dao.repository.EventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.core.env.Environment;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
@KafkaListener(topics = "${dead.letter.topic.name}", groupId = "order-ms")
public class OrderCommandsDltHandler {
    private final Environment env;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final EventRepository eventRepository;

    @KafkaHandler
    @Transactional("jpaTransactionManager")
    public void handleCommand(@Payload ConfirmOrderCommand command,
                              @Header("messageId") String messageId) {
        log.info("Dead message ConfirmOrderCommand received from order-commands topic");

        EventEntity processedEvent = eventRepository.findByMessageId(messageId);
        if (processedEvent != null) {
            log.info("The dead ConfirmOrderCommand message with messageId={} has already been processed", messageId);
            return;
        }

        OrderCompletionFailedEvent failedEvent = new OrderCompletionFailedEvent(
                command.orderId(),
                command.userId(),
                command.amount()
        );

        ProducerRecord<String, Object> record =
                new ProducerRecord<>(
                        env.getRequiredProperty("order.events.topic.name"),
                        command.orderId().toString(),
                        failedEvent
                );
        record.headers().add("messageId", UUID.randomUUID().toString().getBytes());

        kafkaTemplate.send(record);
        log.info("The OrderCompletionFailedEvent message was sent to the order-events topic.");

        eventRepository.save(EventEntity.builder()
                .messageId(messageId)
                .build());
        log.info("Dead message ConfirmOrderCommand with messageId={} has been processed", messageId);
    }
}
