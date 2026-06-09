package by.pressf.orderms.service;

import by.pressf.core.dto.orchestration.events.order.OrderCreatedEvent;
import by.pressf.orderms.dao.entity.OrderEntity;
import by.pressf.orderms.dao.entity.status.OrderStatus;
import by.pressf.orderms.dao.repository.OrderRepository;
import by.pressf.orderms.dto.internal.OrderCreationData;
import by.pressf.orderms.exception.OrderNotFoundException;
import by.pressf.orderms.kafka.publisher.KafkaEventPublisher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NonNull;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Objects;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {
    private final OrderRepository orderRepository;
    private final KafkaEventPublisher kafkaEventPublisher;

    @Transactional("transactionManager")
    public UUID createOrder(@NonNull OrderCreationData creationData) {
        Objects.requireNonNull(creationData, "OrderCreationData must not be null");

        OrderEntity orderEntity = OrderEntity.builder()
                .userId(creationData.userId())
                .productId(creationData.productId())
                .quantity(creationData.quantity())
                .status(OrderStatus.CREATED)
                .build();
        orderRepository.save(orderEntity);
        log.info("A new order has been successfully created. Order ID {}", orderEntity.getId());

        OrderCreatedEvent event = new OrderCreatedEvent(
                orderEntity.getId(),
                creationData.userId(),
                creationData.username(),
                creationData.productId(),
                creationData.quantity()
        );

        kafkaEventPublisher.sendOrderCreatedEvent(event.orderId().toString(), event);

        return orderEntity.getId();
    }

    public void approveOrder(@NonNull UUID orderId) {
        Objects.requireNonNull(orderId, "orderId must not be null");

        OrderEntity entity = orderRepository.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException(orderId));

        entity.setStatus(OrderStatus.APPROVED);

        orderRepository.save(entity);
    }

    public void rejectOrder(@NonNull UUID orderId) {
        Objects.requireNonNull(orderId, "orderId must not be null");

        OrderEntity entity = orderRepository.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException(orderId));

        entity.setStatus(OrderStatus.REJECTED);

        orderRepository.save(entity);
    }
}
