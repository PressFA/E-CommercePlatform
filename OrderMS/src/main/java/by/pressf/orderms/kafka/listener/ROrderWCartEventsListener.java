package by.pressf.orderms.kafka.listener;

import by.pressf.core.dto.orchestration.events.cart.CreateOrderShoppingCart;
import by.pressf.core.exceptions.NotRetryableException;
import by.pressf.orderms.kafka.handler.ROrderWCartEventsHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
@KafkaListener(topics = "${r-order-w-cart.topic.name}", groupId = "order-ms")
public class ROrderWCartEventsListener {
    private final ROrderWCartEventsHandler handler;

    @KafkaHandler
    public void handle(@Payload CreateOrderShoppingCart event,
                       @Header("messageId") String messageId) {
        try {
            log.info("The CreateOrderShoppingCart event from the r-order-w-cart-events topic has been received");

            handler.handleCreateOrderShoppingCart(event, messageId);
        } catch (DataAccessException e) {
            log.error(e.getMessage());
            throw new NotRetryableException(e);
        }
    }
}
