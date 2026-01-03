package by.pressf.orderms.saga;

import by.pressf.core.dto.commands.*;
import by.pressf.core.dto.events.*;
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
}, containerFactory = "sagaKafkaListenerContainerFactory", groupId = "saga-order")
public class OrderSaga {
    private final Environment env;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final OrderHistoryService orderHistoryService;
    private final EventRepository eventRepository;

    /*
    ====================
    Успешные сценарии
    ====================
    */

    @KafkaHandler
    @Transactional("jpaTransactionManager")
    public void handle(@Payload OrderCreatedEvent event,
                       @Header("messageId") String messageId) {
        try {
            log.info("The OrderCreatedEvent event from the order-events topic has been received");

            EventEntity processedEvent = eventRepository.findByMessageId(messageId);
            if (processedEvent != null) {
                log.info("The OrderCreatedEvent message with messageId={} has already been processed", messageId);
                return;
            }

            orderHistoryService.createOrderHistory(event.orderId());
            log.info("SagaOrderd has started creating an order with the ID {}", event.orderId());

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
    @Transactional("jpaTransactionManager")
    public void handle(@Payload ProductReservedEvent event,
                       @Header("messageId") String messageId) {
        try {
            log.info("The ProductReservedEvent event from the product-events topic has been received");

            EventEntity processedEvent = eventRepository.findByMessageId(messageId);
            if (processedEvent != null) {
                log.info("The ProductReservedEvent message with messageId={} has already been processed", messageId);
                return;
            }

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
    @Transactional("jpaTransactionManager")
    public void handle(@Payload PaymentChargedEvent event,
                       @Header("messageId") String messageId) {
        try {
            log.info("The PaymentChargedEvent event from the payment-events topic has been received");

            EventEntity processedEvent = eventRepository.findByMessageId(messageId);
            if (processedEvent != null) {
                log.info("The PaymentChargedEvent message with messageId={} has already been processed", messageId);
                return;
            }

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
    @Transactional("jpaTransactionManager")
    public void handle(@Payload UserBalanceDebitedEvent event,
                       @Header("messageId") String messageId) {
        try {
            log.info("The UserBalanceDebitedEvent event from the user-events topic has been received");

            EventEntity processedEvent = eventRepository.findByMessageId(messageId);
            if (processedEvent != null) {
                log.info("The UserBalanceDebitedEvent message with messageId={} has already been processed", messageId);
                return;
            }

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
    @Transactional("jpaTransactionManager")
    public void handle(@Payload OrderCompletedEvent event,
                       @Header("messageId") String messageId) {
        try {
            log.info("The OrderCompletedEvent event from the order-events topic has been received");

            EventEntity processedEvent = eventRepository.findByMessageId(messageId);
            if (processedEvent != null) {
                log.info("The OrderCompletedEvent message with messageId={} has already been processed", messageId);
                return;
            }

            orderHistoryService.approveOrderHistory(event.orderId());
            log.info("SagaOrderd successfully completed the creation of an order with the {} ID", event.orderId());

            eventRepository.save(EventEntity.builder()
                    .messageId(messageId)
                    .build());
            log.info("The OrderCompletedEvent message with messageId={} has been processed", messageId);
        } catch (DataAccessException e) {
            log.error(e.getMessage());
            throw new NotRetryableException(e);
        }
    }

    /*
    ====================
    Ошибки в сценарии
    ====================
    */

    @KafkaHandler
    @Transactional("jpaTransactionManager")
    public void handle(@Payload ProductReservationFailedEvent event,
                       @Header("messageId") String messageId) {
        try {
            log.info("The ProductReservationFailedEvent event from the product-events topic has been received");

            EventEntity processedEvent = eventRepository.findByMessageId(messageId);
            if (processedEvent != null) {
                log.info("The ProductReservationFailedEvent message with messageId={} has already been processed", messageId);
                return;
            }

            RejectOrderCommand command = new RejectOrderCommand(event.orderId());

            ProducerRecord<String, Object> record =
                    new ProducerRecord<>(
                            env.getRequiredProperty("order.commands.topic.name"),
                            command.orderId().toString(),
                            command
                    );
            record.headers().add("messageId", UUID.randomUUID().toString().getBytes());

            kafkaTemplate.send(record);
            log.info("The RejectOrderCommand message was sent to the order-commands topic.");

            eventRepository.save(EventEntity.builder()
                    .messageId(messageId)
                    .build());
            log.info("The ProductReservationFailedEvent message with messageId={} has been processed", messageId);
        } catch (DataAccessException e) {
            log.error(e.getMessage());
            throw new NotRetryableException(e);
        }
    }

    @KafkaHandler
    @Transactional("jpaTransactionManager")
    public void handle(@Payload PaymentChargeFailedEvent event,
                       @Header("messageId") String messageId) {
        try {
            log.info("The PaymentChargeFailedEvent event from the payment-events topic has been received");

            EventEntity processedEvent = eventRepository.findByMessageId(messageId);
            if (processedEvent != null) {
                log.info("The PaymentChargeFailedEvent message with messageId={} has already been processed", messageId);
                return;
            }

            CancelProductReservationCommand command = new CancelProductReservationCommand(event.orderId());
            ProducerRecord<String, Object> record =
                    new ProducerRecord<>(
                            env.getRequiredProperty("product.commands.topic.name"),
                            command.orderId().toString(),
                            command
                    );
            record.headers().add("messageId", UUID.randomUUID().toString().getBytes());

            kafkaTemplate.send(record);
            log.info("The CancelProductReservationCommand message was sent to the product-commands topic.");

            eventRepository.save(EventEntity.builder()
                    .messageId(messageId)
                    .build());
            log.info("The PaymentChargeFailedEvent message with messageId={} has been processed", messageId);
        } catch (DataAccessException e) {
            log.error(e.getMessage());
            throw new NotRetryableException(e);
        }
    }

    @KafkaHandler
    @Transactional("jpaTransactionManager")
    public void handle(@Payload UserBalanceDebitFailedEvent event,
                       @Header("messageId") String messageId) {
        try {
            log.info("The UserBalanceDebitFailedEvent event from the user-events topic has been received");

            EventEntity processedEvent = eventRepository.findByMessageId(messageId);
            if (processedEvent != null) {
                log.info("The UserBalanceDebitFailedEvent message with messageId={} has already been processed", messageId);
                return;
            }

            RefundPaymentCommand command = new RefundPaymentCommand(event.orderId());

            ProducerRecord<String, Object> record =
                    new ProducerRecord<>(
                            env.getRequiredProperty("payment.commands.topic.name"),
                            command.orderId().toString(),
                            command
                    );
            record.headers().add("messageId", UUID.randomUUID().toString().getBytes());

            kafkaTemplate.send(record);
            log.info("The RefundPaymentCommand message was sent to the payment-commands topic.");

            eventRepository.save(EventEntity.builder()
                    .messageId(messageId)
                    .build());
            log.info("The UserBalanceDebitFailedEvent message with messageId={} has been processed", messageId);
        } catch (DataAccessException e) {
            log.error(e.getMessage());
            throw new NotRetryableException(e);
        }
    }

    @KafkaHandler
    @Transactional("jpaTransactionManager")
    public void handle(@Payload OrderCompletionFailedEvent event,
                       @Header("messageId") String messageId) {
        try {
            log.info("The OrderCompletionFailedEvent event from the order-events topic has been received");

            EventEntity processedEvent = eventRepository.findByMessageId(messageId);
            if (processedEvent != null) {
                log.info("The OrderCompletionFailedEvent message with messageId={} has already been processed", messageId);
                return;
            }

            CancelUserBalanceDebitCommand command = new CancelUserBalanceDebitCommand(
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
            log.info("The CancelUserBalanceDebitCommand message was sent to the user-commands topic.");

            eventRepository.save(EventEntity.builder()
                    .messageId(messageId)
                    .build());
            log.info("The OrderCompletionFailedEvent message with messageId={} has been processed", messageId);
        } catch (DataAccessException e) {
            log.error(e.getMessage());
            throw new NotRetryableException(e);
        }
    }

    /*
    =========================
    Компенсирующие транзакции
    =========================
    */

    @KafkaHandler
    @Transactional("jpaTransactionManager")
    public void handle(@Payload UserBalanceDebitCanceledEvent event,
                       @Header("messageId") String messageId) {
        try {
            log.info("The UserBalanceDebitCanceledEvent event from the user-events topic has been received");

            EventEntity processedEvent = eventRepository.findByMessageId(messageId);
            if (processedEvent != null) {
                log.info("The UserBalanceDebitCanceledEvent message with messageId={} has already been processed", messageId);
                return;
            }

            RefundPaymentCommand command = new RefundPaymentCommand(event.orderID());

            ProducerRecord<String, Object> record =
                    new ProducerRecord<>(
                            env.getRequiredProperty("payment.commands.topic.name"),
                            command.orderId().toString(),
                            command
                    );
            record.headers().add("messageId", UUID.randomUUID().toString().getBytes());

            kafkaTemplate.send(record);
            log.info("The RefundPaymentCommand message was sent to the payment-commands topic");

            eventRepository.save(EventEntity.builder()
                    .messageId(messageId)
                    .build());
            log.info("The UserBalanceDebitCanceledEvent message with messageId={} has been processed", messageId);
        } catch (DataAccessException e) {
            log.error(e.getMessage());
            throw new NotRetryableException(e);
        }
    }

    @KafkaHandler
    @Transactional("jpaTransactionManager")
    public void handle(@Payload PaymentRefundedEvent event,
                       @Header("messageId") String messageId) {
        try {
            log.info("The PaymentRefundedEvent event from the payment-events topic has been received");

            EventEntity processedEvent = eventRepository.findByMessageId(messageId);
            if (processedEvent != null) {
                log.info("The PaymentRefundedEvent message with messageId={} has already been processed", messageId);
                return;
            }

            CancelProductReservationCommand command = new CancelProductReservationCommand(event.orderId());

            ProducerRecord<String, Object> record =
                    new ProducerRecord<>(
                            env.getRequiredProperty("product.commands.topic.name"),
                            command.orderId().toString(),
                            command
                    );
            record.headers().add("messageId", UUID.randomUUID().toString().getBytes());

            kafkaTemplate.send(record);
            log.info("The CancelProductReservationCommand message was sent to the product-commands topic");

            eventRepository.save(EventEntity.builder()
                    .messageId(messageId)
                    .build());
            log.info("The PaymentRefundedEvent message with messageId={} has been processed", messageId);
        } catch (DataAccessException e) {
            log.error(e.getMessage());
            throw new NotRetryableException(e);
        }
    }

    @KafkaHandler
    @Transactional("jpaTransactionManager")
    public void handle(@Payload ProductReservationCanceledEvent event,
                       @Header("messageId") String messageId) {
        try {
            log.info("The ProductReservationCanceledEvent event from the product-events topic has been received");

            EventEntity processedEvent = eventRepository.findByMessageId(messageId);
            if (processedEvent != null) {
                log.info("The ProductReservationCanceledEvent message with messageId={} has already been processed", messageId);
                return;
            }

            RejectOrderCommand command = new RejectOrderCommand(event.orderId());

            ProducerRecord<String, Object> record =
                    new ProducerRecord<>(
                            env.getRequiredProperty("order.commands.topic.name"),
                            command.orderId().toString(),
                            command
                    );
            record.headers().add("messageId", UUID.randomUUID().toString().getBytes());

            kafkaTemplate.send(record);
            log.info("The RejectOrderCommand message was sent to the order-commands topic");

            eventRepository.save(EventEntity.builder()
                    .messageId(messageId)
                    .build());
            log.info("The ProductReservationCanceledEvent message with messageId={} has been processed", messageId);
        } catch (DataAccessException e) {
            log.error(e.getMessage());
            throw new NotRetryableException(e);
        }
    }
}
