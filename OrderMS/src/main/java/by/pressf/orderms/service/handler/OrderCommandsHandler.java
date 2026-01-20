package by.pressf.orderms.service.handler;

import by.pressf.core.dto.commands.ConfirmOrderCommand;
import by.pressf.core.dto.commands.RejectOrderCommand;
import by.pressf.core.dto.events.EmailMessage;
import by.pressf.core.dto.events.OrderCompletedEvent;
import by.pressf.core.dto.events.OrderCompletionFailedEvent;
import by.pressf.core.exceptions.NotRetryableException;
import by.pressf.orderms.dao.entity.EventEntity;
import by.pressf.orderms.dao.repository.EventRepository;
import by.pressf.orderms.exception.OrderNotFoundException;
import by.pressf.orderms.service.OrderService;
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
@KafkaListener(topics = "${order.commands.topic.name}", groupId = "order-ms")
public class OrderCommandsHandler {
    private final Environment env;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final OrderService orderService;
    private final EventRepository eventRepository;

    @KafkaHandler
    @Transactional("transactionManager")
    public void handleCommand(@Payload ConfirmOrderCommand command,
                              @Header("messageId") String messageId) {
        try {
            log.info("The ConfirmOrderCommand command from the order-commands topic has been received");

            EventEntity processedEvent = eventRepository.findByMessageId(messageId);
            if (processedEvent != null) {
                log.info("The ConfirmOrderCommand message with messageId={} has already been processed", messageId);
                return;
            }

            orderService.approveOrder(command.orderId());
            log.info("The order with the ID {} has been approved", command.orderId());

            OrderCompletedEvent event = new OrderCompletedEvent(command.orderId());
            ProducerRecord<String, Object> record1 =
                    new ProducerRecord<>(
                            env.getRequiredProperty("order.events.topic.name"),
                            command.orderId().toString(),
                            event
                    );
            record1.headers().add("messageId", UUID.randomUUID().toString().getBytes());

            kafkaTemplate.send(record1);
            log.info("The OrderCompletedEvent message was sent to the order-events topic.");

            EmailMessage message = new EmailMessage(
                    "artemsurmenok@gmail.com",
                    "TEST subject: APPROVE",
                    "TEST body: APPROVE"
            );

            ProducerRecord<String, Object> record2 =
                    new ProducerRecord<>(
                            env.getRequiredProperty("email-notification.events.topic.name"),
                            command.orderId().toString(),
                            message
                    );
            record2.headers().add("messageId", UUID.randomUUID().toString().getBytes());

            kafkaTemplate.send(record2);
            log.info("The EmailMessage message was sent to the send-notification-event topic.");

            eventRepository.save(EventEntity.builder()
                    .messageId(messageId)
                    .build());
            log.info("The ConfirmOrderCommand message with messageId={} has been processed", messageId);
        } catch (OrderNotFoundException | DataAccessException e) {
            log.error(e.getMessage());

            OrderCompletionFailedEvent failedEvent = new OrderCompletionFailedEvent(
                    command.orderId(),
                    command.userId(),
                    command.amount()
            );

            throw new NotRetryableException(e, env.getRequiredProperty("order.events.topic.name"),
                    command.orderId(), failedEvent);
        }
    }

    @KafkaHandler
    @Transactional("transactionManager")
    public void handleCommand(@Payload RejectOrderCommand command,
                              @Header("messageId") String messageId) {
        try {
            log.info("The RejectOrderCommand command from the order-commands topic has been received");

            EventEntity processedEvent = eventRepository.findByMessageId(messageId);
            if (processedEvent != null) {
                log.info("The RejectOrderCommand message with messageId={} has already been processed", messageId);
                return;
            }

            orderService.rejectOrder(command.orderId());
            log.info("The order with ID {} has been rejected", command.orderId());

            EmailMessage message = new EmailMessage(
                    "artemsurmenok@gmail.com",
                    "TEST subject: REJECT",
                    "TEST body: REJECT"
            );

            ProducerRecord<String, Object> record =
                    new ProducerRecord<>(
                            env.getRequiredProperty("email-notification.events.topic.name"),
                            command.orderId().toString(),
                            message
                    );
            record.headers().add("messageId", UUID.randomUUID().toString().getBytes());

            kafkaTemplate.send(record);
            log.info("The EmailMessage message was sent to the send-notification-event topic.");

            eventRepository.save(EventEntity.builder()
                    .messageId(messageId)
                    .build());
            log.info("The RejectOrderCommand message with messageId={} has been processed", messageId);
        } catch (OrderNotFoundException | DataAccessException e) {
            log.error(e.getMessage());
            throw new NotRetryableException(e);
        }
    }
}
