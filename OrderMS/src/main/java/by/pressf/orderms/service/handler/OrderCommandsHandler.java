package by.pressf.orderms.service.handler;

import by.pressf.core.dto.orchestration.commands.order.ConfirmOrderCommand;
import by.pressf.core.dto.orchestration.commands.order.RejectOrderCommand;
import by.pressf.core.dto.orchestration.events.order.OrderCompletedEvent;
import by.pressf.core.dto.orchestration.events.order.OrderCompletionFailedEvent;
import by.pressf.core.dto.orchestration.events.order.OrderRejectedEvent;
import by.pressf.core.dto.orchestration.events.order.OrderRejectionFailedEvent;
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

            OrderCompletedEvent event = new OrderCompletedEvent(command.orderId(), command.username());
            ProducerRecord<String, Object> record =
                    new ProducerRecord<>(
                            env.getRequiredProperty("successful-events.topic.name"),
                            command.orderId().toString(),
                            event
                    );
            record.headers().add("messageId", UUID.randomUUID().toString().getBytes());

            kafkaTemplate.send(record);
            log.info("The OrderCompletedEvent message was sent to the successful-events topic.");

            eventRepository.save(EventEntity.builder()
                    .messageId(messageId)
                    .build());
            log.info("The ConfirmOrderCommand message with messageId={} has been processed", messageId);
        } catch (OrderNotFoundException | DataAccessException e) {
            log.error(e.getMessage());

            OrderCompletionFailedEvent failedEvent = new OrderCompletionFailedEvent(
                    command.orderId(),
                    command.userId(),
                    command.username(),
                    command.amount()
            );

            throw new NotRetryableException(e, env.getRequiredProperty("errors-successful-events.topic.name"),
                    command.orderId(), failedEvent);
        }
    }

    @KafkaHandler
    @Transactional("transactionManager")
    public void handleCommand(@Payload RejectOrderCommand command,
                              @Header("messageId") String messageId) {
        try {
            log.warn("The RejectOrderCommand command from the order-commands topic has been received");

            EventEntity processedEvent = eventRepository.findByMessageId(messageId);
            if (processedEvent != null) {
                log.warn("The RejectOrderCommand message with messageId={} has already been processed", messageId);
                return;
            }

            orderService.rejectOrder(command.orderId());
            log.warn("The order with ID {} has been rejected", command.orderId());

            OrderRejectedEvent event = new OrderRejectedEvent(command.orderId(), command.username());
            ProducerRecord<String, Object> record =
                    new ProducerRecord<>(
                            env.getRequiredProperty("compensating-events.topic.name"),
                            command.orderId().toString(),
                            event
                    );
            record.headers().add("messageId", UUID.randomUUID().toString().getBytes());

            kafkaTemplate.send(record);
            log.warn("The OrderRejectedEvent message was sent to the compensating-events topic.");

            eventRepository.save(EventEntity.builder()
                    .messageId(messageId)
                    .build());
            log.warn("The RejectOrderCommand message with messageId={} has been processed", messageId);
        } catch (OrderNotFoundException | DataAccessException e) {
            log.error(e.getMessage());

            OrderRejectionFailedEvent failedEvent = new OrderRejectionFailedEvent(command.orderId(), command.username());

            throw new NotRetryableException(e, env.getRequiredProperty("errors-compensating-events.topic.name"),
                    command.orderId(), failedEvent);
        }
    }
}
