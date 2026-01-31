package by.pressf.orderms.service.handler;

import by.pressf.core.dto.events.cart.CreateOrderShoppingCart;
import by.pressf.core.exceptions.NotRetryableException;
import by.pressf.orderms.dao.entity.EventEntity;
import by.pressf.orderms.dao.repository.EventRepository;
import by.pressf.orderms.dto.OrderCreationData;
import by.pressf.orderms.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Component
@RequiredArgsConstructor
@KafkaListener(topics = "${shopping-cart.events.topic.name}", groupId = "order-ms")
public class CartCheckoutListener {
    private final OrderService orderService;
    private final EventRepository eventRepository;

    @KafkaHandler
    @Transactional("transactionManager")
    public void handle(@Payload CreateOrderShoppingCart event,
                       @Header("messageId") String messageId) {
        try {
            log.info("The CreateOrderShoppingCart event from the cart-checkout-initiated topic has been received");

            EventEntity processedEvent = eventRepository.findByMessageId(messageId);
            if (processedEvent != null) {
                log.info("The CreateOrderShoppingCart message with messageId={} has already been processed", messageId);
                return;
            }

            log.info("Received a request from a user with the ID {} to create an order for an item with the ID {}, quantity {}",
                    event.userId(), event.productId(), event.quantity());

            OrderCreationData orderCreationData = new OrderCreationData(
                    event.userId(),
                    event.productId(),
                    event.quantity()
            );

            orderService.createOrder(orderCreationData);

            eventRepository.save(EventEntity.builder()
                    .messageId(messageId)
                    .build());
            log.info("The CreateOrderShoppingCart message with messageId={} has been processed", messageId);
        } catch (DataAccessException e) {
            log.error(e.getMessage());
            throw new NotRetryableException(e);
        }
    }
}
