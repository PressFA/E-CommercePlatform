package by.pressf.paymentms.service.handler;

import by.pressf.core.dto.commands.ChargePaymentCommand;
import by.pressf.core.dto.events.PaymentChargeFailedEvent;
import by.pressf.paymentms.dao.entity.EventEntity;
import by.pressf.paymentms.dao.repository.EventRepository;
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
@KafkaListener(topics = "${dead.letter.topic.name}", groupId = "payment-ms")
public class PaymentCommandsDltHandler {
    private final Environment env;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final EventRepository eventRepository;

    @KafkaHandler
    @Transactional("jpaTransactionManager")
    public void handleDlt(@Payload ChargePaymentCommand command,
                          @Header("messageId") String messageId) {
        log.info("Dead message ChargePaymentCommand received from user-commands topic");

        EventEntity processedEvent = eventRepository.findByMessageId(messageId);
        if (processedEvent != null) {
            log.info("The dead ChargePaymentCommand message with messageId={} has already been processed", messageId);
            return;
        }

        PaymentChargeFailedEvent failedEvent = new PaymentChargeFailedEvent(command.orderId());
        ProducerRecord<String, Object> record =
                new ProducerRecord<>(
                        env.getRequiredProperty("payment.events.topic.name"),
                        command.orderId().toString(),
                        failedEvent
                );
        record.headers().add("messageId", UUID.randomUUID().toString().getBytes());

        kafkaTemplate.send(record);
        log.info("The PaymentChargeFailedEvent message was sent to the payment-events topic.");

        eventRepository.save(EventEntity.builder()
                .messageId(messageId)
                .build());
        log.info("Dead message ChargePaymentCommand with messageId={} has been processed", messageId);
    }
}
