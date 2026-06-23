package by.pressf.orderms.it.kafka.listener;

import by.pressf.core.dto.orchestration.events.cart.CreateOrderShoppingCart;
import by.pressf.core.dto.orchestration.events.order.OrderCreatedEvent;
import by.pressf.orderms.dao.entity.EventEntity;
import by.pressf.orderms.dao.entity.OrderEntity;
import by.pressf.orderms.dao.entity.status.OrderStatus;
import by.pressf.orderms.dao.repository.EventRepository;
import by.pressf.orderms.it.config.BaseIT;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;

public class ROrderWCartEventsIT extends BaseIT {
    @Autowired
    private EventRepository eventRepository;

    @BeforeAll
    static void init() { spyConsumer = createSpyConsumer(List.of("successful-events", "order-ms-dlt")); }

    @BeforeEach
    void setUp() {
        spyConsumer.poll(Duration.ofMillis(100));
        orderRepository.deleteAll();
    }

    @AfterAll
    static void destruct() { spyConsumer.close(); }

    @Test
    void should_CreateOrderAndPublishCreatedEvent_When_EventIsValid() {
        // Arrange
        CreateOrderShoppingCart event = new CreateOrderShoppingCart(
                UUID.randomUUID(),
                "Danny_Black",
                UUID.randomUUID(),
                2
        );

        String rOrderWCartTopic = env.getRequiredProperty("r-order-w-cart.topic.name");
        String successfulEventsTopic = env.getRequiredProperty("successful-events.topic.name");
        String messageId = UUID.randomUUID().toString();

        // Act
        sendMessage(rOrderWCartTopic, event.userId().toString(), event, messageId);

        ConsumerRecord<String, String> record =
                KafkaTestUtils.getSingleRecord(spyConsumer, successfulEventsTopic, Duration.ofSeconds(5));

        // Assert
        assertThat(record.key()).isNotNull();
        assertThat(record.value()).isNotNull();
        assertThat(record.headers().lastHeader("messageId")).isNotNull();

        OrderCreatedEvent publishedEvent = mapper.readValue(record.value(), OrderCreatedEvent.class);
        assertThat(publishedEvent.userId()).isEqualTo(event.userId());
        assertThat(publishedEvent.username()).isEqualTo(event.username());
        assertThat(publishedEvent.productId()).isEqualTo(event.productId());
        assertThat(publishedEvent.quantity()).isEqualTo(event.quantity());

        List<OrderEntity> orders = orderRepository.findAll();
        assertThat(orders).hasSize(1);
        OrderEntity order = orders.getFirst();
        assertThat(order.getId()).isEqualTo(publishedEvent.orderId());
        assertThat(order.getUserId()).isEqualTo(event.userId());
        assertThat(order.getProductId()).isEqualTo(event.productId());
        assertThat(order.getQuantity()).isEqualTo(event.quantity());
        assertThat(order.getStatus()).isEqualTo(OrderStatus.CREATED);

        assertThat(eventRepository.findByMessageId(messageId)).isNotNull();
    }

    @Test
    void should_NotCreateOrderOrPublishEvent_When_MessageIsDuplicate() {
        // Arrange
        String messageId = UUID.randomUUID().toString();
        eventRepository.save(EventEntity.builder().messageId(messageId).build());

        CreateOrderShoppingCart event = new CreateOrderShoppingCart(
                UUID.randomUUID(),
                "Danny_Black",
                UUID.randomUUID(),
                5
        );

        String rOrderWCartTopic = env.getRequiredProperty("r-order-w-cart.topic.name");
        String successfulEventsTopic = env.getRequiredProperty("successful-events.topic.name");

        // Act
        sendMessage(rOrderWCartTopic, event.userId().toString(), event, messageId);

        // Assert
        assertThrows(Exception.class,
                () -> KafkaTestUtils.getSingleRecord(spyConsumer, successfulEventsTopic, Duration.ofSeconds(3)));

        assertThat(orderRepository.findAll()).isEmpty();
    }

    @Test
    void should_PublishToDltImmediatelyWithoutRetries_When_DataAccessExceptionOccurs() {
        // Arrange
        CreateOrderShoppingCart event = new CreateOrderShoppingCart(
                UUID.randomUUID(),
                "Danny_Black",
                UUID.randomUUID(),
                1
        );

        doThrow(new OptimisticLockingFailureException("!!!database error!!!")).when(orderRepository)
                .save(any(OrderEntity.class));

        String rOrderWCartTopic = env.getRequiredProperty("r-order-w-cart.topic.name");
        String dltTopic = "order-ms-dlt";

        // Act
        sendMessage(rOrderWCartTopic, event.userId().toString(), event, UUID.randomUUID().toString());

        ConsumerRecord<String, String> record =
                KafkaTestUtils.getSingleRecord(spyConsumer, dltTopic, Duration.ofSeconds(5));

        // Assert
        assertThat(record.key()).isEqualTo(event.userId().toString());
        assertThat(record.value()).isNotNull();
        assertThat(record.headers().lastHeader("messageId")).isNotNull();

        CreateOrderShoppingCart dltEvent = mapper.readValue(record.value(), CreateOrderShoppingCart.class);
        assertThat(dltEvent.userId()).isEqualTo(event.userId());
        assertThat(dltEvent.username()).isEqualTo(event.username());
        assertThat(dltEvent.productId()).isEqualTo(event.productId());
        assertThat(dltEvent.quantity()).isEqualTo(event.quantity());

        assertThat(orderRepository.findAll()).isEmpty();
    }
}
