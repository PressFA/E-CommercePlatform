package by.pressf.userms.service.handler;

import by.pressf.core.command.DebitUserBalanceCommand;
import by.pressf.core.event.UserBalanceDebitFailedEvent;
import by.pressf.userms.dao.entity.EventEntity;
import by.pressf.userms.dao.repository.EventRepository;
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
@KafkaListener(topics = "${dead.letter.topic.name}", groupId = "user-ms")
public class UserCommandsDltHandler {
    private final Environment env;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final EventRepository eventRepository;

    @KafkaHandler
    @Transactional("jpaTransactionManager")
    public void handleDlt(@Payload DebitUserBalanceCommand command,
                          @Header("messageId") String messageId) {
        log.info("Dead message DebitUserBalanceCommand received from user-commands topic");

        EventEntity processedEvent = eventRepository.findByMessageId(messageId);
        if (processedEvent != null) {
            log.info("The dead DebitUserBalanceCommand message with messageId={} has already been processed", messageId);
            return;
        }

        UserBalanceDebitFailedEvent failedEvent = new UserBalanceDebitFailedEvent(command.orderId());
        ProducerRecord<String, Object> record =
                new ProducerRecord<>(
                        env.getRequiredProperty("user.events.topic.name"),
                        command.orderId().toString(),
                        failedEvent
                );
        record.headers().add("messageId", UUID.randomUUID().toString().getBytes());

        kafkaTemplate.send(record);
        log.info("The UserBalanceDebitFailedEvent message was sent to the user-events topic.");

        eventRepository.save(EventEntity.builder()
                .messageId(messageId)
                .build());
        log.info("Dead message DebitUserBalanceCommand with messageId={} has been processed", messageId);
    }
}
