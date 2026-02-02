package by.pressf.orderms.saga;

import by.pressf.core.dto.commands.emailnotification.SendEmailOrderCommand;
import by.pressf.core.dto.commands.order.RejectOrderCommand;
import by.pressf.core.dto.commands.payment.RefundPaymentCommand;
import by.pressf.core.dto.commands.product.CancelProductReservationCommand;
import by.pressf.core.dto.events.order.OrderRejectionFailedEvent;
import by.pressf.core.dto.events.payment.PaymentRefundFailedEvent;
import by.pressf.core.dto.events.product.ProductReservationCancelFailedEvent;
import by.pressf.core.dto.events.user.UserBalanceDebitCancelFailedEvent;
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
public class OrderSagaCriticalAudit { // Здесь критические события, что компенсация в микросервисе не произошла
    private final Environment env;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final OrderHistoryService orderHistoryService;
    private final EventRepository eventRepository;

    @KafkaHandler
    @Transactional("transactionManager")
    public void handle(@Payload OrderRejectionFailedEvent event,
                       @Header("messageId") String messageId) {
        try {
            log.info("The OrderRejectionFailedEvent event from the order-events topic has been received");

            EventEntity processedEvent = eventRepository.findByMessageId(messageId);
            if (processedEvent != null) {
                log.info("The OrderRejectionFailedEvent message with messageId={} has already been processed", messageId);
                return;
            }

            log.error("An error occurred while compensating for a transaction in the OrderMS for an order with the ID {}",
                    event.orderId());

            orderHistoryService.createFailLog(event.orderId(),
                    "OrderMS(Warning!!!): an error occurred while compensating for a transaction; for more information, see the logs.");

            SendEmailOrderCommand message = new SendEmailOrderCommand(
                    event.username(),
                    "TEST subject: REJECT",
                    "TEST body: REJECT",
                    event.orderId()
            );

            ProducerRecord<String, Object> record =
                    new ProducerRecord<>(
                            env.getRequiredProperty("email-notification.commands.topic.name"),
                            event.orderId().toString(),
                            message
                    );
            record.headers().add("messageId", UUID.randomUUID().toString().getBytes());

            kafkaTemplate.send(record);
            log.info("The SendEmailOrderCommand message was sent to the email-notification-commands topic.");

            eventRepository.save(EventEntity.builder()
                    .messageId(messageId)
                    .build());
            log.info("The OrderRejectionFailedEvent message with messageId={} has been processed", messageId);
        } catch (DataAccessException e) {
            log.error(e.getMessage());
            throw new NotRetryableException(e);
        }
    }

    @KafkaHandler
    @Transactional("transactionManager")
    public void handle(@Payload ProductReservationCancelFailedEvent event,
                       @Header("messageId") String messageId) {
        try {
            log.info("The ProductReservationCanceledEvent event from the product-events topic has been received");

            EventEntity processedEvent = eventRepository.findByMessageId(messageId);
            if (processedEvent != null) {
                log.info("The ProductReservationCanceledEvent message with messageId={} has already been processed", messageId);
                return;
            }

            log.error("An error occurred while compensating for a transaction in the ProductMS for an order with the ID {}",
                    event.orderId());

            orderHistoryService.createFailLog(event.orderId(),
                    "ProductMS(Warning!!!): an error occurred while compensating for a transaction; for more information, see the logs.");

            RejectOrderCommand command = new RejectOrderCommand(event.orderId(), event.username());

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

    @KafkaHandler
    @Transactional("transactionManager")
    public void handle(@Payload PaymentRefundFailedEvent event,
                       @Header("messageId") String messageId) {
        try {
            log.info("The PaymentRefundFailedEvent event from the payment-events topic has been received");

            EventEntity processedEvent = eventRepository.findByMessageId(messageId);
            if (processedEvent != null) {
                log.info("The PaymentRefundFailedEvent message with messageId={} has already been processed", messageId);
                return;
            }

            log.error("An error occurred while compensating for a transaction in the PaymentMS for an order with the ID {}",
                    event.orderId());

            orderHistoryService.createFailLog(event.orderId(),
                    "PaymentMS(Warning!!!): an error occurred while compensating for a transaction; for more information, see the logs.");

            CancelProductReservationCommand command = new CancelProductReservationCommand(event.orderId(), event.username());

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
            log.info("The PaymentRefundFailedEvent message with messageId={} has been processed", messageId);
        } catch (DataAccessException e) {
            log.error(e.getMessage());
            throw new NotRetryableException(e);
        }
    }

    @KafkaHandler
    @Transactional("transactionManager")
    public void handle(@Payload UserBalanceDebitCancelFailedEvent event,
                       @Header("messageId") String messageId) {
        try {
            log.info("The UserBalanceDebitCancelFailedEvent event from the user-events topic has been received");

            EventEntity processedEvent = eventRepository.findByMessageId(messageId);
            if (processedEvent != null) {
                log.info("The UserBalanceDebitCancelFailedEvent message with messageId={} has already been processed", messageId);
                return;
            }

            log.error("An error occurred while compensating for a transaction in the UserMS for an order with the ID {}",
                    event.orderId());

            orderHistoryService.createFailLog(event.orderId(),
                    "UserMS(Warning!!!): an error occurred while compensating for a transaction; for more information, see the logs.");

            RefundPaymentCommand command = new RefundPaymentCommand(event.orderId(), event.username());

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
            log.info("The UserBalanceDebitCancelFailedEvent message with messageId={} has been processed", messageId);
        } catch (DataAccessException e) {
            log.error(e.getMessage());
            throw new NotRetryableException(e);
        }
    }
}
