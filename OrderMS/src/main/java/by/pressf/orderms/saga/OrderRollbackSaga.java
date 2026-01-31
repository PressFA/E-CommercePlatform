package by.pressf.orderms.saga;

import by.pressf.core.dto.commands.order.RejectOrderCommand;
import by.pressf.core.dto.commands.payment.RefundPaymentCommand;
import by.pressf.core.dto.commands.product.CancelProductReservationCommand;
import by.pressf.core.dto.events.emailnotification.EmailMessage;
import by.pressf.core.dto.events.order.OrderRejectedEvent;
import by.pressf.core.dto.events.payment.PaymentRefundedEvent;
import by.pressf.core.dto.events.product.ProductReservationCanceledEvent;
import by.pressf.core.dto.events.user.UserBalanceDebitCanceledEvent;
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
public class OrderRollbackSaga { // Здесь события на продолжение отката
    private final Environment env;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final OrderHistoryService orderHistoryService;
    private final EventRepository eventRepository;

    @KafkaHandler
    @Transactional("transactionManager")
    public void handle(@Payload OrderRejectedEvent event,
                       @Header("messageId") String messageId) {
        try {
            log.info("The OrderRejectedEvent event from the order-events topic has been received");

            EventEntity processedEvent = eventRepository.findByMessageId(messageId);
            if (processedEvent != null) {
                log.info("The OrderRejectedEvent message with messageId={} has already been processed", messageId);
                return;
            }

            orderHistoryService.createSuccessLog(event.orderId(),
                    "OrderMS: the order status has been successfully changed to REJECTED");

            EmailMessage message = new EmailMessage(
                    "artemsurmenok@gmail.com",
                    "TEST subject: REJECT",
                    "TEST body: REJECT"
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
            log.info("The OrderRejectedEvent message with messageId={} has been processed", messageId);
        } catch (DataAccessException e) {
            log.error(e.getMessage());
            throw new NotRetryableException(e);
        }
    }

    @KafkaHandler
    @Transactional("transactionManager")
    public void handle(@Payload ProductReservationCanceledEvent event,
                       @Header("messageId") String messageId) {
        try {
            log.info("The ProductReservationCanceledEvent event from the product-events topic has been received");

            EventEntity processedEvent = eventRepository.findByMessageId(messageId);
            if (processedEvent != null) {
                log.info("The ProductReservationCanceledEvent message with messageId={} has already been processed", messageId);
                return;
            }

            orderHistoryService.createSuccessLog(event.orderId(),
                    "ProductMS: reservation has been successfully lifted from the product");

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

    @KafkaHandler
    @Transactional("transactionManager")
    public void handle(@Payload PaymentRefundedEvent event,
                       @Header("messageId") String messageId) {
        try {
            log.info("The PaymentRefundedEvent event from the payment-events topic has been received");

            EventEntity processedEvent = eventRepository.findByMessageId(messageId);
            if (processedEvent != null) {
                log.info("The PaymentRefundedEvent message with messageId={} has already been processed", messageId);
                return;
            }

            orderHistoryService.createSuccessLog(event.orderId(),
                    "PaymentMS: money has been successfully refunded from the payment");

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
    @Transactional("transactionManager")
    public void handle(@Payload UserBalanceDebitCanceledEvent event,
                       @Header("messageId") String messageId) {
        try {
            log.info("The UserBalanceDebitCanceledEvent event from the user-events topic has been received");

            EventEntity processedEvent = eventRepository.findByMessageId(messageId);
            if (processedEvent != null) {
                log.info("The UserBalanceDebitCanceledEvent message with messageId={} has already been processed", messageId);
                return;
            }

            orderHistoryService.createSuccessLog(event.orderID(),
                    "UserMS: the money was successfully refunded to the user");

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
}
