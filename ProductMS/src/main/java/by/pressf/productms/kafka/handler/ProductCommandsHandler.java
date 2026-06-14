package by.pressf.productms.kafka.handler;

import by.pressf.core.dto.orchestration.commands.product.CancelProductReservationCommand;
import by.pressf.core.dto.orchestration.commands.product.ReserveProductCommand;
import by.pressf.core.dto.orchestration.events.product.ProductReservationCanceledEvent;
import by.pressf.core.dto.orchestration.events.product.ProductReservedEvent;
import by.pressf.productms.dto.internal.ProductReservationRequest;
import by.pressf.productms.kafka.publisher.KafkaEventPublisher;
import by.pressf.productms.service.IdempotencyService;
import by.pressf.productms.service.ProductHistoryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NullMarked;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;

@Slf4j
@Component
@NullMarked
@RequiredArgsConstructor
public class ProductCommandsHandler {
    private final KafkaEventPublisher kafkaEventPublisher;
    private final IdempotencyService idempotencyService;
    private final ProductHistoryService productHistoryService;

    @Transactional("transactionManager")
    public void handleReserveProductCommand(ReserveProductCommand command, String messageId) {
        idempotencyService.idempotenceCheck(messageId, command.getClass().getSimpleName());

        ProductReservationRequest productReservation = new ProductReservationRequest(
                command.orderId(),
                command.productId(),
                command.quantity()
        );

        BigDecimal totalCostProduct = productHistoryService.reserveProduct(productReservation);
        log.info("The product for the order with the ID {} has been successfully reserved", command.orderId());

        ProductReservedEvent event = new ProductReservedEvent(
                command.orderId(),
                command.userId(),
                command.username(),
                totalCostProduct
        );

        kafkaEventPublisher.sendProductReservedEvent(command.orderId().toString(), event);

        idempotencyService.saveIdempotentKey(messageId, command.getClass().getSimpleName());
    }

    @Transactional("transactionManager")
    public void handleCancelProductReservationCommand(CancelProductReservationCommand command, String messageId) {
        idempotencyService.idempotenceCheck(messageId, command.getClass().getSimpleName());

        productHistoryService.cancelProductReservation(command.orderId());
        log.warn("The product from the order with ID {} has been removed from the reservation", command.orderId());

        ProductReservationCanceledEvent event = new ProductReservationCanceledEvent(
                command.orderId(),
                command.username()
        );

        kafkaEventPublisher.sendProductReservationCanceledEvent(command.orderId().toString(), event);

        idempotencyService.saveIdempotentKey(messageId, command.getClass().getSimpleName());
    }
}
