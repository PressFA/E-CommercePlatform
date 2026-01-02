package by.pressf.paymentms.service.handler;

import by.pressf.core.dto.commands.ChargePaymentCommand;
import by.pressf.core.dto.events.PaymentChargedEvent;
import by.pressf.core.exceptions.NotRetryableException;
import by.pressf.core.exceptions.RetryableException;
import by.pressf.paymentms.dao.entity.EventEntity;
import by.pressf.paymentms.dao.repository.EventRepository;
import by.pressf.paymentms.dto.CreateOrderPaymentRequest;
import by.pressf.paymentms.exception.PaymentFailedException;
import by.pressf.paymentms.service.PaymentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.core.env.Environment;
import org.springframework.dao.DataAccessException;
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
@KafkaListener(topics = "${payment.commands.topic.name}", groupId = "payment-ms")
public class PaymentCommandsHandler {
    private final Environment env;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final PaymentService paymentService;
    private final EventRepository eventRepository;

    @KafkaHandler
    @Transactional("jpaTransactionManager")
    public void handleCommand(@Payload ChargePaymentCommand command,
                              @Header("messageId") String messageId) {
        try {
            log.info("The ChargePaymentCommand command from the payment-commands topic has been received");

            EventEntity processedEvent = eventRepository.findByMessageId(messageId);
            if (processedEvent != null) {
                log.info("The ChargePaymentCommand message with messageId={} has already been processed", messageId);
                return;
            }

            CreateOrderPaymentRequest request = new CreateOrderPaymentRequest(
                    command.orderId(),
                    command.userId(),
                    command.amount()
            );

            paymentService.createOrderPayment(request);
            log.info("The payment for the order with the {} ID was successful", command.orderId());

            PaymentChargedEvent event = new PaymentChargedEvent(
                    command.orderId(),
                    command.userId(),
                    command.amount()
            );

            ProducerRecord<String, Object> record =
                    new ProducerRecord<>(
                            env.getRequiredProperty("payment.events.topic.name"),
                            command.orderId().toString(),
                            event
                    );
            record.headers().add("messageId", UUID.randomUUID().toString().getBytes());

            kafkaTemplate.send(record);
            log.info("The PaymentChargedEvent message was sent to the payment-events topic.");

            eventRepository.save(EventEntity.builder()
                    .messageId(messageId)
                    .build());
            log.info("The ChargePaymentCommand message with messageId={} has been processed", messageId);
        } catch (PaymentFailedException e) {
            log.error(e.getMessage());
            switch (e.getStatusCode()) {
                // ошибка со стороны нашего сервиса
                case 0 -> {
                    log.error("Error on the part of our service");
                    throw new NotRetryableException(e);
                }
                // постоянная ошибка
                case 400, 401, 402, 403, 404 -> throw new NotRetryableException(e);
                // временная ошибка
                case 409, 424, 429, 500, 502, 503, 504 -> throw new RetryableException(e);
                // неизвестная ошибка
                default -> {
                    log.error("Unknown error");
                    throw new NotRetryableException(e);
                }
            }
        } catch (DataAccessException e) {
            log.error(e.getMessage());
            throw new NotRetryableException(e);
        }
    }
}
