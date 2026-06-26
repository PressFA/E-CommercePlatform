package by.pressf.orderms.saga.listener;

import by.pressf.core.dto.orchestration.events.order.OrderCompletionFailedEvent;
import by.pressf.core.dto.orchestration.events.payment.PaymentChargeFailedEvent;
import by.pressf.core.dto.orchestration.events.product.ProductReservationFailedEvent;
import by.pressf.core.dto.orchestration.events.user.UserBalanceDebitFailedEvent;
import by.pressf.core.exceptions.DuplicateMessageException;
import by.pressf.core.exceptions.NotRetryableException;
import by.pressf.orderms.saga.handler.CompensationSagaHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
@KafkaListener(topics = "${errors-successful-events.topic.name}", groupId = "order-compensation-saga")
public class CompensationSagaListener { // Здесь события, когда отправляем первые команды на откат
    private final CompensationSagaHandler handler;

    @KafkaHandler
    public void handle(@Payload ProductReservationFailedEvent event,
                       @Header("messageId") String messageId) {
        try {
            log.warn("The ProductReservationFailedEvent event from the errors-successful-events topic has been received");

            handler.handleProductReservationFailedEvent(event, messageId);
        } catch (DuplicateMessageException e) {
            log.warn(e.getMessage());
        } catch (DataAccessException e) {
            log.error(e.getMessage());
            throw new NotRetryableException(e);
        }
    }

    @KafkaHandler
    public void handle(@Payload PaymentChargeFailedEvent event,
                       @Header("messageId") String messageId) {
        try {
            log.warn("The PaymentChargeFailedEvent event from the errors-successful-events topic has been received");

            handler.handlePaymentChargeFailedEvent(event, messageId);
        } catch (DuplicateMessageException e) {
            log.warn(e.getMessage());
        } catch (DataAccessException e) {
            log.error(e.getMessage());
            throw new NotRetryableException(e);
        }
    }

    @KafkaHandler
    public void handle(@Payload UserBalanceDebitFailedEvent event,
                       @Header("messageId") String messageId) {
        try {
            log.warn("The UserBalanceDebitFailedEvent event from the errors-successful-events topic has been received");

            handler.handleUserBalanceDebitFailedEvent(event, messageId);
        } catch (DuplicateMessageException e) {
            log.warn(e.getMessage());
        } catch (DataAccessException e) {
            log.error(e.getMessage());
            throw new NotRetryableException(e);
        }
    }

    @KafkaHandler
    public void handle(@Payload OrderCompletionFailedEvent event,
                       @Header("messageId") String messageId) {
        try {
            log.warn("The OrderCompletionFailedEvent event from the errors-successful-events topic has been received");

            handler.handleOrderCompletionFailedEvent(event, messageId);
        } catch (DuplicateMessageException e) {
            log.warn(e.getMessage());
        } catch (DataAccessException e) {
            log.error(e.getMessage());
            throw new NotRetryableException(e);
        }
    }
}
