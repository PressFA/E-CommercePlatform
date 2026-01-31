package by.pressf.orderms.saga;

import by.pressf.core.dto.commands.order.ConfirmOrderCommand;
import by.pressf.core.dto.commands.payment.ChargePaymentCommand;
import by.pressf.core.dto.commands.product.ReserveProductCommand;
import by.pressf.core.dto.commands.user.DebitUserBalanceCommand;
import by.pressf.core.dto.events.emailnotification.EmailMessage;
import by.pressf.core.dto.events.order.OrderCompletedEvent;
import by.pressf.core.dto.events.order.OrderCreatedEvent;
import by.pressf.core.dto.events.payment.PaymentChargedEvent;
import by.pressf.core.dto.events.product.ProductReservedEvent;
import by.pressf.core.dto.events.user.UserBalanceDebitedEvent;
import by.pressf.core.exceptions.NotRetryableException;
import by.pressf.orderms.dao.entity.EventEntity;
import by.pressf.orderms.dao.repository.EventRepository;
import by.pressf.orderms.service.OrderHistoryService;
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
@KafkaListener(topics = {
        "${order.events.topic.name}",
        "${product.events.topic.name}",
        "${payment.events.topic.name}",
        "${user.events.topic.name}"
}, groupId = "saga-order")
public class OrderForwardSaga { // Здесь события, когда всё идёт идеально
    private final Environment env;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final OrderHistoryService orderHistoryService;
    private final EventRepository eventRepository;

    @KafkaHandler
    @Transactional("transactionManager")
    public void handle(@Payload OrderCreatedEvent event,
                       @Header("messageId") String messageId) {
        try {
            log.info("The OrderCreatedEvent event from the order-events topic has been received");

            EventEntity processedEvent = eventRepository.findByMessageId(messageId);
            if (processedEvent != null) {
                log.info("The OrderCreatedEvent message with messageId={} has already been processed", messageId);
                return;
            }

            orderHistoryService.createSuccessLog(event.orderId(),
                    "OrderMS: starting to form an order for the user");

            ReserveProductCommand command = new ReserveProductCommand(
                    event.orderId(),
                    event.productId(),
                    event.userId(),
                    event.quantity()
            );

            ProducerRecord<String, Object> record =
                    new ProducerRecord<>(
                            env.getRequiredProperty("product.commands.topic.name"),
                            command.orderId().toString(),
                            command
                    );
            record.headers().add("messageId", UUID.randomUUID().toString().getBytes());

            kafkaTemplate.send(record);
            log.info("The ReserveProductCommand message was sent to the product-commands topic.");

            eventRepository.save(EventEntity.builder()
                    .messageId(messageId)
                    .build());
            log.info("The OrderCreatedEvent message with messageId={} has been processed", messageId);
        } catch (DataAccessException e) {
            log.error(e.getMessage());
            throw new NotRetryableException(e);
        }
    }

    @KafkaHandler
    @Transactional("transactionManager")
    public void handle(@Payload ProductReservedEvent event,
                       @Header("messageId") String messageId) {
        try {
            log.info("The ProductReservedEvent event from the product-events topic has been received");

            EventEntity processedEvent = eventRepository.findByMessageId(messageId);
            if (processedEvent != null) {
                log.info("The ProductReservedEvent message with messageId={} has already been processed", messageId);
                return;
            }

            orderHistoryService.createSuccessLog(event.orderId(),
                    "ProductMS: the product is reserved");

            ChargePaymentCommand command = new ChargePaymentCommand(
                    event.orderId(),
                    event.userId(),
                    event.amount()
            );

            ProducerRecord<String, Object> record =
                    new ProducerRecord<>(
                            env.getRequiredProperty("payment.commands.topic.name"),
                            command.orderId().toString(),
                            command
                    );
            record.headers().add("messageId", UUID.randomUUID().toString().getBytes());

            kafkaTemplate.send(record);
            log.info("The ChargePaymentCommand message was sent to the payment-commands topic.");

            eventRepository.save(EventEntity.builder()
                    .messageId(messageId)
                    .build());
            log.info("The ProductReservedEvent message with messageId={} has been processed", messageId);
        } catch (DataAccessException e) {
            log.error(e.getMessage());
            throw new NotRetryableException(e);
        }
    }

    @KafkaHandler
    @Transactional("transactionManager")
    public void handle(@Payload PaymentChargedEvent event,
                       @Header("messageId") String messageId) {
        try {
            log.info("The PaymentChargedEvent event from the payment-events topic has been received");

            EventEntity processedEvent = eventRepository.findByMessageId(messageId);
            if (processedEvent != null) {
                log.info("The PaymentChargedEvent message with messageId={} has already been processed", messageId);
                return;
            }

            orderHistoryService.createSuccessLog(event.orderId(),
                    "PaymentMS: the payment was successful");

            DebitUserBalanceCommand command = new DebitUserBalanceCommand(
                    event.orderId(),
                    event.userId(),
                    event.amount()
            );

            ProducerRecord<String, Object> record =
                    new ProducerRecord<>(
                            env.getRequiredProperty("user.commands.topic.name"),
                            command.orderId().toString(),
                            command
                    );
            record.headers().add("messageId", UUID.randomUUID().toString().getBytes());

            kafkaTemplate.send(record);
            log.info("The DebitUserBalanceCommand message was sent to the user-commands topic.");

            eventRepository.save(EventEntity.builder()
                    .messageId(messageId)
                    .build());
            log.info("The PaymentChargedEvent message with messageId={} has been processed", messageId);
        } catch (DataAccessException e) {
            log.error(e.getMessage());
            throw new NotRetryableException(e);
        }
    }

    @KafkaHandler
    @Transactional("transactionManager")
    public void handle(@Payload UserBalanceDebitedEvent event,
                       @Header("messageId") String messageId) {
        try {
            log.info("The UserBalanceDebitedEvent event from the user-events topic has been received");

            EventEntity processedEvent = eventRepository.findByMessageId(messageId);
            if (processedEvent != null) {
                log.info("The UserBalanceDebitedEvent message with messageId={} has already been processed", messageId);
                return;
            }

            orderHistoryService.createSuccessLog(event.orderId(),
                    "UserMS: the user's balance has been successfully changed");

            ConfirmOrderCommand command = new ConfirmOrderCommand(
                    event.orderId(),
                    event.userId(),
                    event.amount()
            );

            ProducerRecord<String, Object> record =
                    new ProducerRecord<>(
                            env.getRequiredProperty("order.commands.topic.name"),
                            command.orderId().toString(),
                            command
                    );
            record.headers().add("messageId", UUID.randomUUID().toString().getBytes());

            kafkaTemplate.send(record);
            log.info("The ConfirmOrderCommand message was sent to the order-commands topic.");

            eventRepository.save(EventEntity.builder()
                    .messageId(messageId)
                    .build());
            log.info("The UserBalanceDebitedEvent message with messageId={} has been processed", messageId);
        } catch (DataAccessException e) {
            log.error(e.getMessage());
            throw new NotRetryableException(e);
        }
    }

    @KafkaHandler
    @Transactional("transactionManager")
    public void handle(@Payload OrderCompletedEvent event,
                       @Header("messageId") String messageId) {
        try {
            log.info("The OrderCompletedEvent event from the order-events topic has been received");

            EventEntity processedEvent = eventRepository.findByMessageId(messageId);
            if (processedEvent != null) {
                log.info("The OrderCompletedEvent message with messageId={} has already been processed", messageId);
                return;
            }

            orderHistoryService.createSuccessLog(event.orderId(),
                    "OrderMS: The order status has been successfully changed to APPROVED");

            EmailMessage message = new EmailMessage(
                    "artemsurmenok@gmail.com",
                    "TEST subject: APPROVE",
                    "TEST body: APPROVE"
            );

            ProducerRecord<String, Object> record =
                    new ProducerRecord<>(
                            env.getRequiredProperty("email-notification.events.topic.name"),
                            event.orderId().toString(),
                            message
                    );
            record.headers().add("messageId", UUID.randomUUID().toString().getBytes());

            kafkaTemplate.send(record);
            log.info("The EmailMessage message was sent to the send-notification-event topic.");

            eventRepository.save(EventEntity.builder()
                    .messageId(messageId)
                    .build());
            log.info("The OrderCompletedEvent message with messageId={} has been processed", messageId);
        } catch (DataAccessException e) {
            log.error(e.getMessage());
            throw new NotRetryableException(e);
        }
    }
}
