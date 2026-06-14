package by.pressf.orderms.kafka.handler;

import by.pressf.core.dto.orchestration.events.cart.CreateOrderShoppingCart;
import by.pressf.orderms.dto.internal.OrderCreationData;
import by.pressf.orderms.service.IdempotencyService;
import by.pressf.orderms.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NullMarked;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Component
@NullMarked
@RequiredArgsConstructor
public class ROrderWCartEventsHandler {
    private final OrderService orderService;
    private final IdempotencyService idempotencyService;

    @Transactional("transactionManager")
    public void handleCreateOrderShoppingCart(CreateOrderShoppingCart event, String messageId) {
        idempotencyService.idempotenceCheck(messageId, event.getClass().getSimpleName());

        log.info("Received a request from a user with the ID {} to create an order for an item with the ID {}, quantity {}",
                event.userId(), event.productId(), event.quantity());

        OrderCreationData orderCreationData = new OrderCreationData(
                event.userId(),
                event.username(),
                event.productId(),
                event.quantity()
        );

        orderService.createOrder(orderCreationData);

        idempotencyService.saveIdempotentKey(messageId, event.getClass().getSimpleName());
    }
}
