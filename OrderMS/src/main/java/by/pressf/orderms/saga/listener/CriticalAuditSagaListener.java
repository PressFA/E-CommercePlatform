package by.pressf.orderms.saga.listener;

import by.pressf.core.dto.orchestration.events.order.OrderRejectionFailedEvent;
import by.pressf.core.dto.orchestration.events.payment.PaymentRefundFailedEvent;
import by.pressf.core.dto.orchestration.events.product.ProductReservationCancelFailedEvent;
import by.pressf.core.dto.orchestration.events.user.UserBalanceDebitCancelFailedEvent;
import by.pressf.core.exceptions.NotRetryableException;
import by.pressf.orderms.saga.handler.CriticalAuditSagaHandler;
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
@KafkaListener(topics = "${errors-compensating-events.topic.name}", groupId = "order-audit-saga")
public class CriticalAuditSagaListener { // Здесь критические события, что компенсация в микросервисе не произошла
    private final CriticalAuditSagaHandler handler;

    @KafkaHandler
    public void handle(@Payload OrderRejectionFailedEvent event,
                       @Header("messageId") String messageId) {
        try {
            log.error("The OrderRejectionFailedEvent event from the errors-compensating-events topic has been received");

            handler.handleOrderRejectionFailedEvent(event, messageId);
        } catch (DataAccessException e) {
            log.error(e.getMessage());
            throw new NotRetryableException(e);
        }
    }

    @KafkaHandler
    public void handle(@Payload ProductReservationCancelFailedEvent event,
                       @Header("messageId") String messageId) {
        try {
            log.error("The ProductReservationCanceledEvent event from the errors-compensating-events topic has been received");

            handler.handleProductReservationCancelFailedEvent(event, messageId);
        } catch (DataAccessException e) {
            log.error(e.getMessage());
            throw new NotRetryableException(e);
        }
    }

    @KafkaHandler
    public void handle(@Payload PaymentRefundFailedEvent event,
                       @Header("messageId") String messageId) {
        try {
            log.error("The PaymentRefundFailedEvent event from the errors-compensating-events topic has been received");

            handler.handlePaymentRefundFailedEvent(event, messageId);
        } catch (DataAccessException e) {
            log.error(e.getMessage());
            throw new NotRetryableException(e);
        }
    }

    @KafkaHandler
    public void handle(@Payload UserBalanceDebitCancelFailedEvent event,
                       @Header("messageId") String messageId) {
        try {
            log.error("The UserBalanceDebitCancelFailedEvent event from the errors-compensating-events topic has been received");

            handler.handleUserBalanceDebitCancelFailedEvent(event, messageId);
        } catch (DataAccessException e) {
            log.error(e.getMessage());
            throw new NotRetryableException(e);
        }
    }
}
