package by.pressf.orderms.kafka.listener;

import by.pressf.core.dto.orchestration.commands.order.ConfirmOrderCommand;
import by.pressf.core.dto.orchestration.commands.order.RejectOrderCommand;
import by.pressf.core.dto.orchestration.events.order.OrderCompletionFailedEvent;
import by.pressf.core.dto.orchestration.events.order.OrderRejectionFailedEvent;
import by.pressf.core.exceptions.NotRetryableException;
import by.pressf.orderms.exception.OrderNotFoundException;
import by.pressf.orderms.kafka.handler.OrderCommandsHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.env.Environment;
import org.springframework.dao.DataAccessException;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
@KafkaListener(topics = "${order.commands.topic.name}", groupId = "order-ms")
public class OrderCommandsListener {
    private final Environment env;
    private final OrderCommandsHandler handler;

    @KafkaHandler
    public void handleCommand(@Payload ConfirmOrderCommand command,
                              @Header("messageId") String messageId) {
        try {
            log.info("The ConfirmOrderCommand command from the order-commands topic has been received");

            handler.handleConfirmOrderCommand(command, messageId);
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
    public void handleCommand(@Payload RejectOrderCommand command,
                              @Header("messageId") String messageId) {
        try {
            log.warn("The RejectOrderCommand command from the order-commands topic has been received");

            handler.handleRejectOrderCommand(command, messageId);
        } catch (OrderNotFoundException | DataAccessException e) {
            log.error(e.getMessage());

            OrderRejectionFailedEvent failedEvent = new OrderRejectionFailedEvent(command.orderId(), command.username());

            throw new NotRetryableException(e, env.getRequiredProperty("errors-compensating-events.topic.name"),
                    command.orderId(), failedEvent);
        }
    }
}
