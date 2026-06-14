package by.pressf.orderms.saga.listener;

import by.pressf.core.dto.orchestration.events.order.OrderCompletedEvent;
import by.pressf.core.dto.orchestration.events.order.OrderCreatedEvent;
import by.pressf.core.dto.orchestration.events.payment.PaymentChargedEvent;
import by.pressf.core.dto.orchestration.events.product.ProductReservedEvent;
import by.pressf.core.dto.orchestration.events.user.UserBalanceDebitedEvent;
import by.pressf.core.exceptions.DuplicateMessageException;
import by.pressf.core.exceptions.NotRetryableException;
import by.pressf.orderms.saga.handler.ForwardSagaHandler;
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
@KafkaListener(topics = "${successful-events.topic.name}", groupId = "order-forward-saga")
public class ForwardSagaListener { // Здесь события, когда всё идёт идеально
    private final ForwardSagaHandler handler;

    @KafkaHandler
    public void handle(@Payload OrderCreatedEvent event,
                       @Header("messageId") String messageId) {
        try {
            log.info("The OrderCreatedEvent event from the successful-events topic has been received");

            handler.handleOrderCreatedEvent(event, messageId);
        } catch (DuplicateMessageException e) {
            log.warn(e.getMessage());
        } catch (DataAccessException e) {
            log.error(e.getMessage());
            throw new NotRetryableException(e);
        }
    }

    @KafkaHandler
    public void handle(@Payload ProductReservedEvent event,
                       @Header("messageId") String messageId) {
        try {
            log.info("The ProductReservedEvent event from the successful-events topic has been received");

            handler.handleProductReservedEvent(event, messageId);
        } catch (DuplicateMessageException e) {
            log.warn(e.getMessage());
        } catch (DataAccessException e) {
            log.error(e.getMessage());
            throw new NotRetryableException(e);
        }
    }

    @KafkaHandler
    public void handle(@Payload PaymentChargedEvent event,
                       @Header("messageId") String messageId) {
        try {
            log.info("The PaymentChargedEvent event from the successful-events topic has been received");

            handler.handlePaymentChargedEvent(event, messageId);
        } catch (DuplicateMessageException e) {
            log.warn(e.getMessage());
        } catch (DataAccessException e) {
            log.error(e.getMessage());
            throw new NotRetryableException(e);
        }
    }

    @KafkaHandler
    public void handle(@Payload UserBalanceDebitedEvent event,
                       @Header("messageId") String messageId) {
        try {
            log.info("The UserBalanceDebitedEvent event from the successful-events topic has been received");

            handler.handleUserBalanceDebitedEvent(event, messageId);
        } catch (DuplicateMessageException e) {
            log.warn(e.getMessage());
        } catch (DataAccessException e) {
            log.error(e.getMessage());
            throw new NotRetryableException(e);
        }
    }

    @KafkaHandler
    public void handle(@Payload OrderCompletedEvent event,
                       @Header("messageId") String messageId) {
        try {
            log.info("The OrderCompletedEvent event from the successful-events topic has been received");

            handler.handleOrderCompletedEvent(event, messageId);
        } catch (DuplicateMessageException e) {
            log.warn(e.getMessage());
        } catch (DataAccessException e) {
            log.error(e.getMessage());
            throw new NotRetryableException(e);
        }
    }
}
