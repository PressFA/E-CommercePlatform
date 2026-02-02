package by.pressf.orderms.service;

import by.pressf.core.dto.orchestration.events.order.OrderCreatedEvent;
import by.pressf.orderms.dao.entity.OrderEntity;
import by.pressf.orderms.dao.entity.status.OrderStatus;
import by.pressf.orderms.dao.repository.OrderRepository;
import by.pressf.orderms.dto.OrderCreationData;
import by.pressf.orderms.exception.OrderNotFoundException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {
    private final Environment env;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final OrderRepository orderRepository;

    @Transactional("transactionManager")
    public UUID createOrder(OrderCreationData creationData) {
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

        ProducerRecord<String, Object> record =
                new ProducerRecord<>(
                        env.getRequiredProperty("order.events.topic.name"),
                        event.orderId().toString(),
                        event
                );
        record.headers().add("messageId", UUID.randomUUID().toString().getBytes());

        kafkaTemplate.send(record);
        log.info("The OrderCreatedEvent message was sent to the order-events topic.");

        return orderEntity.getId();
    }

    public void approveOrder(UUID orderId) {
        OrderEntity entity = orderRepository.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException(orderId));

        entity.setStatus(OrderStatus.APPROVED);

        orderRepository.save(entity);
    }

    public void rejectOrder(UUID orderId) {
        OrderEntity entity = orderRepository.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException(orderId));

        entity.setStatus(OrderStatus.REJECTED);

        orderRepository.save(entity);
    }
}
