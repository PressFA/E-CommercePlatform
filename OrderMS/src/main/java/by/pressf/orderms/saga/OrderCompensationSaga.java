package by.pressf.orderms.saga;

import by.pressf.core.dto.commands.emailnotification.SendEmailOrderCommand;
import by.pressf.core.dto.commands.order.RejectOrderCommand;
import by.pressf.core.dto.commands.payment.RefundPaymentCommand;
import by.pressf.core.dto.commands.product.CancelProductReservationCommand;
import by.pressf.core.dto.events.emailnotification.EmailOrderNotSentEvent;
import by.pressf.core.dto.events.order.OrderCompletionFailedEvent;
import by.pressf.core.dto.events.payment.PaymentChargeFailedEvent;
import by.pressf.core.dto.events.product.ProductReservationFailedEvent;
import by.pressf.core.dto.events.user.UserBalanceDebitFailedEvent;
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
        "${user.events.topic.name}",
        "${email-notification.events.topic.name}"
}, groupId = "saga-order")
public class OrderCompensationSaga { // Здесь события, когда отправляем первые команды на откат
    private final Environment env;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final OrderHistoryService orderHistoryService;
    private final EventRepository eventRepository;

    @KafkaHandler
    @Transactional("transactionManager")
    public void handle(@Payload ProductReservationFailedEvent event,
                       @Header("messageId") String messageId) {
        try {
            log.info("The ProductReservationFailedEvent event from the product-events topic has been received");

            EventEntity processedEvent = eventRepository.findByMessageId(messageId);
            if (processedEvent != null) {
                log.info("The ProductReservationFailedEvent message with messageId={} has already been processed", messageId);
                return;
            }

            orderHistoryService.createFailLog(event.orderId(),
                    "ProductMS: the product could not be booked; for more information, see the logs.");

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
    @Transactional("transactionManager")
    public void handle(@Payload PaymentChargeFailedEvent event,
                       @Header("messageId") String messageId) {
        try {
            log.info("The PaymentChargeFailedEvent event from the payment-events topic has been received");

            EventEntity processedEvent = eventRepository.findByMessageId(messageId);
            if (processedEvent != null) {
                log.info("The PaymentChargeFailedEvent message with messageId={} has already been processed", messageId);
                return;
            }

            orderHistoryService.createFailLog(event.orderId(),
                    "PaymentMS: couldn't pay for the product; for more information, see the logs.");

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
    @Transactional("transactionManager")
    public void handle(@Payload UserBalanceDebitFailedEvent event,
                       @Header("messageId") String messageId) {
        try {
            log.info("The UserBalanceDebitFailedEvent event from the user-events topic has been received");

            EventEntity processedEvent = eventRepository.findByMessageId(messageId);
            if (processedEvent != null) {
                log.info("The UserBalanceDebitFailedEvent message with messageId={} has already been processed", messageId);
                return;
            }

            orderHistoryService.createFailLog(event.orderId(),
                    "UserMS: couldn't change user's balance; for more information, see the logs.");

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
    @Transactional("transactionManager")
    public void handle(@Payload OrderCompletionFailedEvent event,
                       @Header("messageId") String messageId) {
        try {
            log.info("The OrderCompletionFailedEvent event from the order-events topic has been received");

            EventEntity processedEvent = eventRepository.findByMessageId(messageId);
            if (processedEvent != null) {
                log.info("The OrderCompletionFailedEvent message with messageId={} has already been processed", messageId);
                return;
            }

            orderHistoryService.createFailLog(event.orderId(),
                    "OrderMS: couldn't change the order status to APPROVED; for more information, see the logs.");

            SendEmailOrderCommand command = new SendEmailOrderCommand(
                    "artemsurmenok@gmail.com",
                    "TEST subject: APPROVE",
                    "TEST body: APPROVE",
                    event.orderId()
            );

            ProducerRecord<String, Object> record =
                    new ProducerRecord<>(
                            env.getRequiredProperty("email-notification.commands.topic.name"),
                            event.orderId().toString(),
                            command
                    );
            record.headers().add("messageId", UUID.randomUUID().toString().getBytes());

            kafkaTemplate.send(record);
            log.info("The SendEmailOrderCommand message was sent to the email-notification-commands topic.");

            eventRepository.save(EventEntity.builder()
                    .messageId(messageId)
                    .build());
            log.info("The OrderCompletionFailedEvent message with messageId={} has been processed", messageId);
        } catch (DataAccessException e) {
            log.error(e.getMessage());
            throw new NotRetryableException(e);
        }
    }

    @KafkaHandler
    @Transactional("transactionManager")
    public void handle(@Payload EmailOrderNotSentEvent event,
                       @Header("messageId") String messageId) {
        try {
            log.info("The EmailOrderNotSentEvent event from the email-notification-events topic has been received");

            EventEntity processedEvent = eventRepository.findByMessageId(messageId);
            if (processedEvent != null) {
                log.info("The EmailOrderNotSentEvent message with messageId={} has already been processed", messageId);
                return;
            }

            orderHistoryService.createFailLog(event.orderId(),
                    "EmailNotificationMS: couldn't send the user an email.");

            orderHistoryService.createSuccessLog(event.orderId(),
                    "OrderSaga: saga has completed its work");

            eventRepository.save(EventEntity.builder()
                    .messageId(messageId)
                    .build());
            log.info("The EmailOrderNotSentEvent message with messageId={} has been processed", messageId);
        } catch (DataAccessException e) {
            log.error(e.getMessage());
            throw new NotRetryableException(e);
        }
    }
}
