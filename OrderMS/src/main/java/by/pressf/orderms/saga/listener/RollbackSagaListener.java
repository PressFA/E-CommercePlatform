package by.pressf.orderms.saga.listener;

import by.pressf.core.dto.orchestration.events.order.OrderRejectedEvent;
import by.pressf.core.dto.orchestration.events.payment.PaymentRefundedEvent;
import by.pressf.core.dto.orchestration.events.product.ProductReservationCanceledEvent;
import by.pressf.core.dto.orchestration.events.user.UserBalanceDebitCanceledEvent;
import by.pressf.core.exceptions.DuplicateMessageException;
import by.pressf.core.exceptions.NotRetryableException;
import by.pressf.orderms.saga.handler.RollbackSagaHandler;
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
@KafkaListener(topics = "${compensating-events.topic.name}", groupId = "order-rollback-saga")
public class RollbackSagaListener { // Здесь события на продолжение отката
    private final RollbackSagaHandler handler;

    @KafkaHandler
    public void handle(@Payload OrderRejectedEvent event,
                       @Header("messageId") String messageId) {
        try {
            log.warn("The OrderRejectedEvent event from the compensating-events topic has been received");

            handler.handleOrderRejectedEvent(event, messageId);
        } catch (DuplicateMessageException e) {
            log.warn(e.getMessage());
        } catch (DataAccessException e) {
            log.error(e.getMessage());
            throw new NotRetryableException(e);
        }
    }

    @KafkaHandler
    public void handle(@Payload ProductReservationCanceledEvent event,
                       @Header("messageId") String messageId) {
        try {
            log.warn("The ProductReservationCanceledEvent event from the compensating-events topic has been received");

            handler.handleProductReservationCanceledEvent(event, messageId);
        } catch (DuplicateMessageException e) {
            log.warn(e.getMessage());
        } catch (DataAccessException e) {
            log.error(e.getMessage());
            throw new NotRetryableException(e);
        }
    }

    @KafkaHandler
    public void handle(@Payload PaymentRefundedEvent event,
                       @Header("messageId") String messageId) {
        try {
            log.warn("The PaymentRefundedEvent event from the compensating-events topic has been received");

            handler.handlePaymentRefundedEvent(event, messageId);
        } catch (DuplicateMessageException e) {
            log.warn(e.getMessage());
        } catch (DataAccessException e) {
            log.error(e.getMessage());
            throw new NotRetryableException(e);
        }
    }

    @KafkaHandler
    public void handle(@Payload UserBalanceDebitCanceledEvent event,
                       @Header("messageId") String messageId) {
        try {
            log.warn("The UserBalanceDebitCanceledEvent event from the compensating-events topic has been received");

            handler.handleUserBalanceDebitCanceledEvent(event, messageId);
        } catch (DuplicateMessageException e) {
            log.warn(e.getMessage());
        } catch (DataAccessException e) {
            log.error(e.getMessage());
            throw new NotRetryableException(e);
        }
    }
}
