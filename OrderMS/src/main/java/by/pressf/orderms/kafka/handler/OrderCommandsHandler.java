package by.pressf.orderms.kafka.handler;

import by.pressf.core.dto.orchestration.commands.order.ConfirmOrderCommand;
import by.pressf.core.dto.orchestration.commands.order.RejectOrderCommand;
import by.pressf.core.dto.orchestration.events.order.OrderCompletedEvent;
import by.pressf.core.dto.orchestration.events.order.OrderRejectedEvent;
import by.pressf.orderms.kafka.publisher.KafkaEventPublisher;
import by.pressf.orderms.service.IdempotencyService;
import by.pressf.orderms.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NonNull;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Objects;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderCommandsHandler {
    private final OrderService orderService;
    private final KafkaEventPublisher kafkaEventPublisher;
    private final IdempotencyService idempotencyService;

    @Transactional("transactionManager")
    public void handleConfirmOrderCommand(@NonNull ConfirmOrderCommand command,
                                          @NonNull String messageId) {
        Objects.requireNonNull(command);
        Objects.requireNonNull(messageId);

        idempotencyService.idempotenceCheck(messageId, command.getClass().getSimpleName());

        orderService.approveOrder(command.orderId());
        log.info("The order with the ID {} has been approved", command.orderId());

        OrderCompletedEvent event = new OrderCompletedEvent(command.orderId(), command.username());

        kafkaEventPublisher.sendOrderCompletedEvent(command.orderId().toString(), event);

        idempotencyService.saveIdempotentKey(messageId, command.getClass().getSimpleName());
    }

    @Transactional("transactionManager")
    public void handleRejectOrderCommand(@NonNull RejectOrderCommand command,
                                         @NonNull String messageId) {
        Objects.requireNonNull(command);
        Objects.requireNonNull(messageId);

        idempotencyService.idempotenceCheck(messageId, command.getClass().getSimpleName());

        orderService.rejectOrder(command.orderId());
        log.warn("The order with ID {} has been rejected", command.orderId());

        OrderRejectedEvent event = new OrderRejectedEvent(command.orderId(), command.username());

        kafkaEventPublisher.sendOrderRejectedEvent(command.orderId().toString(), event);

        idempotencyService.saveIdempotentKey(messageId, command.getClass().getSimpleName());
    }
}
