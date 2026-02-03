package by.pressf.paymentms.service.handler;

import by.pressf.core.dto.orchestration.commands.payment.RefundPaymentCommand;
import by.pressf.core.dto.orchestration.commands.payment.ChargePaymentCommand;
import by.pressf.core.dto.orchestration.events.payment.PaymentChargeFailedEvent;
import by.pressf.core.dto.orchestration.events.payment.PaymentChargedEvent;
import by.pressf.core.dto.orchestration.events.payment.PaymentRefundFailedEvent;
import by.pressf.core.dto.orchestration.events.payment.PaymentRefundedEvent;
import by.pressf.core.exceptions.NotRetryableException;
import by.pressf.core.exceptions.RetryableException;
import by.pressf.paymentms.dao.entity.EventEntity;
import by.pressf.paymentms.dao.repository.EventRepository;
import by.pressf.paymentms.dto.CreateOrderPaymentRequest;
import by.pressf.paymentms.dto.RefundPaymentRequest;
import by.pressf.paymentms.exception.PaymentFailedException;
import by.pressf.paymentms.exception.PaymentNotFoundByOrderIdException;
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
    @Transactional("transactionManager")
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
                    messageId,
                    command.orderId(),
                    command.userId(),
                    command.amount()
            );

            paymentService.createOrderPayment(request);
            log.info("The payment for the order with the {} ID was successful", command.orderId());

            PaymentChargedEvent event = new PaymentChargedEvent(
                    command.orderId(),
                    command.userId(),
                    command.username(),
                    command.amount()
            );

            ProducerRecord<String, Object> record =
                    new ProducerRecord<>(
                            env.getRequiredProperty("successful-events.topic.name"),
                            command.orderId().toString(),
                            event
                    );
            record.headers().add("messageId", UUID.randomUUID().toString().getBytes());

            kafkaTemplate.send(record);
            log.info("The PaymentChargedEvent message was sent to the successful-events topic.");

            eventRepository.save(EventEntity.builder()
                    .messageId(messageId)
                    .build());
            log.info("The ChargePaymentCommand message with messageId={} has been processed", messageId);
        } catch (PaymentFailedException e) {
            log.error(e.getMessage());

            PaymentChargeFailedEvent failedEvent = createChargeFailedEvent(command);

            throw handleStripeException(e, env.getRequiredProperty("errors-successful-events.topic.name"),
                    command.orderId(), failedEvent);
        } catch (PaymentNotFoundByOrderIdException | DataAccessException e) {
            log.error(e.getMessage());

            PaymentChargeFailedEvent failedEvent = createChargeFailedEvent(command);

            throw new NotRetryableException(e, env.getRequiredProperty("errors-successful-events.topic.name"),
                    command.orderId(), failedEvent);
        }
    }

    @KafkaHandler
    @Transactional("transactionManager")
    public void handleCommand(@Payload RefundPaymentCommand command,
                              @Header("messageId") String messageId) {
        try {
            log.warn("The RefundPaymentCommand command from the payment-commands topic has been received");

            EventEntity processedEvent = eventRepository.findByMessageId(messageId);
            if (processedEvent != null) {
                log.warn("The RefundPaymentCommand message with messageId={} has already been processed", messageId);
                return;
            }

            RefundPaymentRequest refundPaymentRequest = new RefundPaymentRequest(messageId, command.orderId());

            paymentService.refundOrderPayment(refundPaymentRequest);
            log.warn("The refund for order ID {} has been successfully processed", command.orderId());

            PaymentRefundedEvent event = new PaymentRefundedEvent(command.orderId(), command.username());
            ProducerRecord<String, Object> record =
                    new ProducerRecord<>(
                            env.getRequiredProperty("compensating-events.topic.name"),
                            command.orderId().toString(),
                            event
                    );
            record.headers().add("messageId", UUID.randomUUID().toString().getBytes());

            kafkaTemplate.send(record);
            log.warn("The PaymentRefundedEvent message was sent to the compensating-events topic.");

            eventRepository.save(EventEntity.builder()
                    .messageId(messageId)
                    .build());
            log.warn("The RefundPaymentCommand message with messageId={} has been processed", messageId);
        } catch (PaymentFailedException e) {
            log.error(e.getMessage());

            PaymentRefundFailedEvent failedEvent = createRefundFailedEvent(command);

            throw handleStripeException(e, env.getRequiredProperty("errors-compensating-events.topic.name"),
                    command.orderId(), failedEvent);
        } catch (PaymentNotFoundByOrderIdException | DataAccessException e) {
            log.error(e.getMessage());

            PaymentRefundFailedEvent failedEvent = createRefundFailedEvent(command);

            throw new NotRetryableException(e, env.getRequiredProperty("errors-compensating-events.topic.name"),
                    command.orderId(), failedEvent);
        }
    }

    private <T> RuntimeException handleStripeException(PaymentFailedException e, String topic, UUID orderId, T failedEvent) {
        RuntimeException returnEx;
        switch (e.getStatusCode()) {
            case 400, 401, 402, 403, 404 ->
                    returnEx = new RetryableException(e, topic, orderId, failedEvent);
            default -> {
                if (e.getStatusCode() == 0) log.error("Error on the part of our service");
                returnEx = new NotRetryableException(e, topic, orderId, failedEvent);
            }
        }
        return returnEx;
    }

    private PaymentChargeFailedEvent createChargeFailedEvent(ChargePaymentCommand command) {
        return new PaymentChargeFailedEvent(command.orderId(), command.username());
    }

    private PaymentRefundFailedEvent createRefundFailedEvent(RefundPaymentCommand command) {
        return new PaymentRefundFailedEvent(command.orderId(), command.username());
    }
}
