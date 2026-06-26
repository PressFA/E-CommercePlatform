package by.pressf.orderms.it.saga.listener;

import by.pressf.core.dto.orchestration.events.order.OrderCompletedEvent;
import by.pressf.core.dto.orchestration.events.order.OrderCreatedEvent;
import by.pressf.core.dto.orchestration.events.payment.PaymentChargedEvent;
import by.pressf.core.dto.orchestration.events.product.ProductReservedEvent;
import by.pressf.core.dto.orchestration.events.user.UserBalanceDebitedEvent;
import by.pressf.orderms.dao.entity.OrderEntity;
import by.pressf.orderms.dao.entity.OrderHistoryEntity;
import by.pressf.orderms.dao.entity.status.OrderHistoryStatus;
import by.pressf.orderms.dao.entity.status.OrderStatus;
import by.pressf.orderms.dao.repository.OrderHistoryRepository;
import by.pressf.orderms.it.config.BaseIT;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ForwardSagaListenerIT extends BaseIT {
    @Autowired
    private OrderHistoryRepository orderHistoryRepository;

    @BeforeAll
    static void init() {
        spyConsumer = createSpyConsumer(List.of("product-commands", "payment-commands", "user-commands",
                "order-commands", "email-notification-commands", "order-ms-dlt"));
    }

    @BeforeEach
    void setUp() {
        spyConsumer.poll(Duration.ofMillis(100));
        orderHistoryRepository.deleteAll();
        orderRepository.deleteAll();
    }

    @AfterAll
    static void destruct() { spyConsumer.close(); }

    @Test @Order(1)
    void should_HandleOrderCreatedEvent_Successfully() {
        OrderEntity order = orderRepository.save(OrderEntity.builder()
                .userId(UUID.randomUUID())
                .productId(UUID.randomUUID())
                .quantity(5)
                .status(OrderStatus.CREATED)
                .build());

        OrderCreatedEvent event = new OrderCreatedEvent(
                order.getId(),
                UUID.randomUUID(),
                "user@test.com",
                UUID.randomUUID(),
                2
        );
        String topic = env.getRequiredProperty("successful-events.topic.name");
        String messageId = UUID.randomUUID().toString();

        sendMessage(topic, event.orderId().toString(), event, messageId);

        ConsumerRecord<String, String> record =
                KafkaTestUtils.getSingleRecord(spyConsumer, "product-commands", Duration.ofSeconds(5));
        assertThat(record.value()).contains(event.orderId().toString());

        List<OrderHistoryEntity> history = orderHistoryRepository.findAll().stream()
                .filter(h -> h.getOrderId().equals(event.orderId()))
                .toList();

//        assertThat(history).hasSize(1);
        assertThat(history.getFirst().getStatus()).isEqualTo(OrderHistoryStatus.SUCCESS);
        assertThat(history.getFirst().getReason())
                .isEqualTo("OrderMS: starting to form an order for the user");
    }

    @Test @Order(3)
    void should_HandleProductReservedEvent_Successfully() {
        OrderEntity order = orderRepository.save(OrderEntity.builder()
                .userId(UUID.randomUUID())
                .productId(UUID.randomUUID())
                .quantity(5)
                .status(OrderStatus.CREATED)
                .build());

        ProductReservedEvent event = new ProductReservedEvent(
                order.getId(),
                UUID.randomUUID(),
                "user@test.com",
                BigDecimal.TEN
        );
        String topic = env.getRequiredProperty("successful-events.topic.name");
        String messageId = UUID.randomUUID().toString();

        sendMessage(topic, event.orderId().toString(), event, messageId);

        ConsumerRecord<String, String> record =
                KafkaTestUtils.getSingleRecord(spyConsumer, "payment-commands", Duration.ofSeconds(5));
        assertThat(record.value()).contains(event.orderId().toString());

        List<OrderHistoryEntity> history = orderHistoryRepository.findAll().stream()
                .filter(h -> h.getOrderId().equals(event.orderId()))
                .toList();

//        assertThat(history).hasSize(1);
        assertThat(history.getFirst().getReason()).isEqualTo("ProductMS: the product is reserved");
    }

    @Test @Order(5)
    void should_HandlePaymentChargedEvent_Successfully() {
        OrderEntity order = orderRepository.save(OrderEntity.builder()
                .userId(UUID.randomUUID())
                .productId(UUID.randomUUID())
                .quantity(5)
                .status(OrderStatus.CREATED)
                .build());

        PaymentChargedEvent event = new PaymentChargedEvent(
                order.getId(),
                UUID.randomUUID(),
                "user@test.com",
                BigDecimal.TEN
        );
        String topic = env.getRequiredProperty("successful-events.topic.name");
        String messageId = UUID.randomUUID().toString();

        sendMessage(topic, event.orderId().toString(), event, messageId);

        ConsumerRecord<String, String> record =
                KafkaTestUtils.getSingleRecord(spyConsumer, "user-commands", Duration.ofSeconds(5));
        assertThat(record.value()).contains(event.orderId().toString());

        List<OrderHistoryEntity> history = orderHistoryRepository.findAll().stream()
                .filter(h -> h.getOrderId().equals(event.orderId()))
                .toList();

//        assertThat(history).hasSize(1);
        assertThat(history.getFirst().getReason()).isEqualTo("PaymentMS: the payment was successful");
    }

    @Test @Order(7)
    void should_HandleUserBalanceDebitedEvent_Successfully() {
        OrderEntity order = orderRepository.save(OrderEntity.builder()
                .userId(UUID.randomUUID())
                .productId(UUID.randomUUID())
                .quantity(5)
                .status(OrderStatus.CREATED)
                .build());

        UserBalanceDebitedEvent event = new UserBalanceDebitedEvent(
                order.getId(),
                UUID.randomUUID(),
                "user@test.com",
                BigDecimal.TEN
        );
        String topic = env.getRequiredProperty("successful-events.topic.name");
        String messageId = UUID.randomUUID().toString();

        sendMessage(topic, event.orderId().toString(), event, messageId);

        ConsumerRecord<String, String> record =
                KafkaTestUtils.getSingleRecord(spyConsumer, "order-commands", Duration.ofSeconds(5));
        assertThat(record.value()).contains(event.orderId().toString());

        List<OrderHistoryEntity> history = orderHistoryRepository.findAll().stream()
                .filter(h -> h.getOrderId().equals(event.orderId()))
                .toList();

//        assertThat(history).hasSize(1);
        assertThat(history.getFirst().getReason())
                .isEqualTo("UserMS: the user's balance has been successfully changed");
    }

    @Test @Order(9)
    void should_HandleOrderCompletedEvent_Successfully() {
        OrderEntity order = orderRepository.save(OrderEntity.builder()
                .userId(UUID.randomUUID())
                .productId(UUID.randomUUID())
                .quantity(5)
                .status(OrderStatus.CREATED)
                .build());

        OrderCompletedEvent event = new OrderCompletedEvent(
                order.getId(),
                "user@test.com"
        );
        String topic = env.getRequiredProperty("successful-events.topic.name");
        String messageId = UUID.randomUUID().toString();

        sendMessage(topic, event.orderId().toString(), event, messageId);

        ConsumerRecord<String, String> record =
                KafkaTestUtils.getSingleRecord(spyConsumer, "email-notification-commands", Duration.ofSeconds(5));
        assertThat(record.value()).contains(event.orderId().toString());

        List<OrderHistoryEntity> history = orderHistoryRepository.findAll().stream()
                .filter(h -> h.getOrderId().equals(event.orderId()))
                .toList();

//        assertThat(history).hasSize(2);
        assertThat(history.get(0).getReason())
                .isEqualTo("OrderMS: The order status has been successfully changed to APPROVED");
        assertThat(history.get(1).getReason())
                .isEqualTo("OrderSaga: saga has completed its work");
    }

    @Test @Order(2)
    void should_PublishToDltAndNotToProductCommands_When_DataAccessExceptionOccursDuringOrderCreated() {
        // Arrange
        OrderEntity order = orderRepository.save(OrderEntity.builder()
                .userId(UUID.randomUUID())
                .productId(UUID.randomUUID())
                .quantity(5)
                .status(OrderStatus.CREATED)
                .build());

        OrderCreatedEvent event = new OrderCreatedEvent(
                order.getId(),
                UUID.randomUUID(),
                "user@test.com",
                UUID.randomUUID(),
                2
        );
        String topic = env.getRequiredProperty("successful-events.topic.name");
        String messageId = UUID.randomUUID().toString();

        doThrow(new OptimisticLockingFailureException("Database write failed"))
                .when(orderHistoryRepository).save(any(OrderHistoryEntity.class));

        // Act
        sendMessage(topic, event.orderId().toString(), event, messageId);

        ConsumerRecord<String, String> dltRecord =
                KafkaTestUtils.getSingleRecord(spyConsumer, "order-ms-dlt", Duration.ofSeconds(5));

        // Assert
        OrderCreatedEvent dltEvent = mapper.readValue(dltRecord.value(), OrderCreatedEvent.class);
        assertThat(dltEvent.orderId()).isEqualTo(event.orderId());
        assertThat(dltEvent.username()).isEqualTo(event.username());

        assertThrows(Exception.class,
                () -> KafkaTestUtils.getSingleRecord(spyConsumer, "product-commands", Duration.ofSeconds(1)));
    }

    @Test @Order(4)
    void should_PublishToDltAndNotToPaymentCommands_When_DataAccessExceptionOccursDuringProductReserved() {
        // Arrange
        OrderEntity order = orderRepository.save(OrderEntity.builder()
                .userId(UUID.randomUUID())
                .productId(UUID.randomUUID())
                .quantity(5)
                .status(OrderStatus.CREATED)
                .build());

        ProductReservedEvent event = new ProductReservedEvent(
                order.getId(),
                UUID.randomUUID(),
                "user@test.com",
                BigDecimal.TEN
        );
        String topic = env.getRequiredProperty("successful-events.topic.name");
        String messageId = UUID.randomUUID().toString();

        doThrow(new OptimisticLockingFailureException("Database write failed"))
                .when(orderHistoryRepository).save(any(OrderHistoryEntity.class));

        // Act
        sendMessage(topic, event.orderId().toString(), event, messageId);

        ConsumerRecord<String, String> dltRecord =
                KafkaTestUtils.getSingleRecord(spyConsumer, "order-ms-dlt", Duration.ofSeconds(5));

        // Assert
        ProductReservedEvent dltEvent = mapper.readValue(dltRecord.value(), ProductReservedEvent.class);
        assertThat(dltEvent.orderId()).isEqualTo(event.orderId());
        assertThat(dltEvent.username()).isEqualTo(event.username());

        assertThrows(Exception.class,
                () -> KafkaTestUtils.getSingleRecord(spyConsumer, "payment-commands", Duration.ofSeconds(1)));
    }

    @Test @Order(6)
    void should_PublishToDltAndNotToUserCommands_When_DataAccessExceptionOccursDuringPaymentCharged() {
        // Arrange
        OrderEntity order = orderRepository.save(OrderEntity.builder()
                .userId(UUID.randomUUID())
                .productId(UUID.randomUUID())
                .quantity(5)
                .status(OrderStatus.CREATED)
                .build());

        PaymentChargedEvent event = new PaymentChargedEvent(
                order.getId(),
                UUID.randomUUID(),
                "user@test.com",
                BigDecimal.TEN
        );
        String topic = env.getRequiredProperty("successful-events.topic.name");
        String messageId = UUID.randomUUID().toString();

        doThrow(new OptimisticLockingFailureException("Database write failed"))
                .when(orderHistoryRepository).save(any(OrderHistoryEntity.class));

        // Act
        sendMessage(topic, event.orderId().toString(), event, messageId);

        ConsumerRecord<String, String> dltRecord =
                KafkaTestUtils.getSingleRecord(spyConsumer, "order-ms-dlt", Duration.ofSeconds(5));

        // Assert
        PaymentChargedEvent dltEvent = mapper.readValue(dltRecord.value(), PaymentChargedEvent.class);
        assertThat(dltEvent.orderId()).isEqualTo(event.orderId());
        assertThat(dltEvent.username()).isEqualTo(event.username());

        assertThrows(Exception.class,
                () -> KafkaTestUtils.getSingleRecord(spyConsumer, "user-commands", Duration.ofSeconds(1)));
    }

    @Test @Order(8)
    void should_PublishToDltAndNotToOrderCommands_When_DataAccessExceptionOccursDuringUserBalanceDebited() {
        // Arrange
        OrderEntity order = orderRepository.save(OrderEntity.builder()
                .userId(UUID.randomUUID())
                .productId(UUID.randomUUID())
                .quantity(5)
                .status(OrderStatus.CREATED)
                .build());

        UserBalanceDebitedEvent event = new UserBalanceDebitedEvent(
                order.getId(),
                UUID.randomUUID(),
                "user@test.com",
                BigDecimal.TEN
        );
        String topic = env.getRequiredProperty("successful-events.topic.name");
        String messageId = UUID.randomUUID().toString();

        doThrow(new OptimisticLockingFailureException("Database write failed"))
                .when(orderHistoryRepository).save(any(OrderHistoryEntity.class));

        // Act
        sendMessage(topic, event.orderId().toString(), event, messageId);

        ConsumerRecord<String, String> dltRecord =
                KafkaTestUtils.getSingleRecord(spyConsumer, "order-ms-dlt", Duration.ofSeconds(5));

        // Assert
        UserBalanceDebitedEvent dltEvent = mapper.readValue(dltRecord.value(), UserBalanceDebitedEvent.class);
        assertThat(dltEvent.orderId()).isEqualTo(event.orderId());
        assertThat(dltEvent.username()).isEqualTo(event.username());

        assertThrows(Exception.class,
                () -> KafkaTestUtils.getSingleRecord(spyConsumer, "order-commands", Duration.ofSeconds(1)));
    }

    @Test @Order(10)
    void should_PublishToDltAndNotToEmailNotificationCommands_When_DataAccessExceptionOccursDuringOrderCompleted() {
        // Arrange
        OrderEntity order = orderRepository.save(OrderEntity.builder()
                .userId(UUID.randomUUID())
                .productId(UUID.randomUUID())
                .quantity(5)
                .status(OrderStatus.CREATED)
                .build());

        OrderCompletedEvent event = new OrderCompletedEvent(
                order.getId(),
                "user@test.com"
        );
        String topic = env.getRequiredProperty("successful-events.topic.name");
        String messageId = UUID.randomUUID().toString();

        doThrow(new OptimisticLockingFailureException("Database write failed"))
                .when(orderHistoryRepository).save(any(OrderHistoryEntity.class));

        // Act
        sendMessage(topic, event.orderId().toString(), event, messageId);

        ConsumerRecord<String, String> dltRecord =
                KafkaTestUtils.getSingleRecord(spyConsumer, "order-ms-dlt", Duration.ofSeconds(5));

        // Assert
        OrderCompletedEvent dltEvent = mapper.readValue(dltRecord.value(), OrderCompletedEvent.class);
        assertThat(dltEvent.orderId()).isEqualTo(event.orderId());
        assertThat(dltEvent.username()).isEqualTo(event.username());

        assertThrows(Exception.class,
                () -> KafkaTestUtils.getSingleRecord(spyConsumer, "email-notification-commands", Duration.ofSeconds(1)));
    }
}
