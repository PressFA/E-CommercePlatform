package by.pressf.orderms.it.saga.listener;

import by.pressf.core.dto.orchestration.events.order.OrderCompletionFailedEvent;
import by.pressf.core.dto.orchestration.events.payment.PaymentChargeFailedEvent;
import by.pressf.core.dto.orchestration.events.product.ProductReservationFailedEvent;
import by.pressf.core.dto.orchestration.events.user.UserBalanceDebitFailedEvent;
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
public class CompensationSagaListenerIT extends BaseIT {
    @Autowired
    private OrderHistoryRepository orderHistoryRepository;

    @BeforeAll
    static void init() {
        spyConsumer = createSpyConsumer(List.of("order-commands", "product-commands", "payment-commands",
                "user-commands", "order-ms-dlt"));
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
    void should_HandleProductReservationFailedEvent_Successfully() {
        OrderEntity order = orderRepository.save(OrderEntity.builder()
                .userId(UUID.randomUUID())
                .productId(UUID.randomUUID())
                .quantity(5)
                .status(OrderStatus.CREATED)
                .build());

        ProductReservationFailedEvent event = new ProductReservationFailedEvent(
                order.getId(),
                "user@test.com"
        );

        String topic = env.getRequiredProperty("errors-successful-events.topic.name");
        String messageId = UUID.randomUUID().toString();

        sendMessage(topic, event.orderId().toString(), event, messageId);

        ConsumerRecord<String, String> record =
                KafkaTestUtils.getSingleRecord(spyConsumer, "order-commands", Duration.ofSeconds(5));
        assertThat(record.value()).contains(event.orderId().toString());

        List<OrderHistoryEntity> history = orderHistoryRepository.findAll().stream()
                .filter(h -> h.getOrderId().equals(event.orderId()))
                .toList();

//        assertThat(history).hasSize(1);
        assertThat(history.getFirst().getStatus()).isEqualTo(OrderHistoryStatus.FAIL);
        assertThat(history.getFirst().getReason())
                .isEqualTo("ProductMS: the product could not be booked; for more information, see the logs.");
    }

    @Test @Order(3)
    void should_HandlePaymentChargeFailedEvent_Successfully() {
        OrderEntity order = orderRepository.save(OrderEntity.builder()
                .userId(UUID.randomUUID())
                .productId(UUID.randomUUID())
                .quantity(5)
                .status(OrderStatus.CREATED)
                .build());

        PaymentChargeFailedEvent event = new PaymentChargeFailedEvent(
                order.getId(),
                "user@test.com"
        );
        String topic = env.getRequiredProperty("errors-successful-events.topic.name");
        String messageId = UUID.randomUUID().toString();

        sendMessage(topic, event.orderId().toString(), event, messageId);

        ConsumerRecord<String, String> record =
                KafkaTestUtils.getSingleRecord(spyConsumer, "product-commands", Duration.ofSeconds(5));
        assertThat(record.value()).contains(event.orderId().toString());

        List<OrderHistoryEntity> history = orderHistoryRepository.findAll().stream()
                .filter(h -> h.getOrderId().equals(event.orderId()))
                .toList();

//        assertThat(history).hasSize(1);
        assertThat(history.getFirst().getStatus()).isEqualTo(OrderHistoryStatus.FAIL);
        assertThat(history.getFirst().getReason())
                .isEqualTo("PaymentMS: couldn't pay for the product; for more information, see the logs.");
    }

    @Test @Order(5)
    void should_HandleUserBalanceDebitFailedEvent_Successfully() {
        OrderEntity order = orderRepository.save(OrderEntity.builder()
                .userId(UUID.randomUUID())
                .productId(UUID.randomUUID())
                .quantity(5)
                .status(OrderStatus.CREATED)
                .build());

        UserBalanceDebitFailedEvent event = new UserBalanceDebitFailedEvent(
                order.getId(),
                "user@test.com"
        );
        String topic = env.getRequiredProperty("errors-successful-events.topic.name");
        String messageId = UUID.randomUUID().toString();

        sendMessage(topic, event.orderId().toString(), event, messageId);

        ConsumerRecord<String, String> record =
                KafkaTestUtils.getSingleRecord(spyConsumer, "payment-commands", Duration.ofSeconds(5));
        assertThat(record.value()).contains(event.orderId().toString());

        List<OrderHistoryEntity> history = orderHistoryRepository.findAll().stream()
                .filter(h -> h.getOrderId().equals(event.orderId()))
                .toList();

//        assertThat(history).hasSize(1);
        assertThat(history.getFirst().getReason())
                .isEqualTo("UserMS: couldn't change user's balance; for more information, see the logs.");
    }

    @Test @Order(7)
    void should_HandleOrderCompletionFailedEvent_Successfully() {
        OrderEntity order = orderRepository.save(OrderEntity.builder()
                .userId(UUID.randomUUID())
                .productId(UUID.randomUUID())
                .quantity(5)
                .status(OrderStatus.CREATED)
                .build());

        OrderCompletionFailedEvent event = new OrderCompletionFailedEvent(
                order.getId(),
                UUID.randomUUID(),
                "user@test.com",
                BigDecimal.TEN
        );
        String topic = env.getRequiredProperty("errors-successful-events.topic.name");
        String messageId = UUID.randomUUID().toString();

        sendMessage(topic, event.orderId().toString(), event, messageId);

        ConsumerRecord<String, String> record =
                KafkaTestUtils.getSingleRecord(spyConsumer, "user-commands", Duration.ofSeconds(5));
        assertThat(record.value()).contains(event.orderId().toString());

        List<OrderHistoryEntity> history = orderHistoryRepository.findAll().stream()
                .filter(h -> h.getOrderId().equals(event.orderId()))
                .toList();

//        assertThat(history).hasSize(1);
        assertThat(history.getFirst().getReason())
                .isEqualTo("OrderMS: couldn't change the order status to APPROVED; for more information, see the logs.");
    }

    @Test @Order(2)
    void should_PublishToDltAndNotToOrderCommands_When_DataAccessExceptionOccursDuringProductReservationFailed() {
        // Arrange
        OrderEntity order = orderRepository.save(OrderEntity.builder()
                .userId(UUID.randomUUID())
                .productId(UUID.randomUUID())
                .quantity(5)
                .status(OrderStatus.CREATED)
                .build());

        ProductReservationFailedEvent event = new ProductReservationFailedEvent(
                order.getId(),
                "user@test.com"
        );
        String topic = env.getRequiredProperty("errors-successful-events.topic.name");
        String messageId = UUID.randomUUID().toString();

        doThrow(new OptimisticLockingFailureException("Database crash on history log"))
                .when(orderHistoryRepository).save(any(OrderHistoryEntity.class));

        // Act
        sendMessage(topic, event.orderId().toString(), event, messageId);

        ConsumerRecord<String, String> dltRecord =
                KafkaTestUtils.getSingleRecord(spyConsumer, "order-ms-dlt", Duration.ofSeconds(5));

        // Assert
        ProductReservationFailedEvent dltEvent =
                mapper.readValue(dltRecord.value(), ProductReservationFailedEvent.class);
        assertThat(dltEvent.orderId()).isEqualTo(event.orderId());
        assertThat(dltEvent.username()).isEqualTo(event.username());

        assertThrows(Exception.class,
                () -> KafkaTestUtils.getSingleRecord(spyConsumer, "order-commands", Duration.ofSeconds(1)));
    }

    @Test @Order(4)
    void should_PublishToDltAndNotToProductCommands_When_DataAccessExceptionOccursDuringPaymentChargeFailed() {
        // Arrange
        OrderEntity order = orderRepository.save(OrderEntity.builder()
                .userId(UUID.randomUUID())
                .productId(UUID.randomUUID())
                .quantity(5)
                .status(OrderStatus.CREATED)
                .build());

        PaymentChargeFailedEvent event = new PaymentChargeFailedEvent(
                order.getId(),
                "user@test.com"
        );
        String topic = env.getRequiredProperty("errors-successful-events.topic.name");
        String messageId = UUID.randomUUID().toString();

        doThrow(new OptimisticLockingFailureException("Database crash on history log"))
                .when(orderHistoryRepository).save(any(OrderHistoryEntity.class));

        // Act
        sendMessage(topic, event.orderId().toString(), event, messageId);

        ConsumerRecord<String, String> dltRecord =
                KafkaTestUtils.getSingleRecord(spyConsumer, "order-ms-dlt", Duration.ofSeconds(5));

        // Assert
        PaymentChargeFailedEvent dltEvent = mapper.readValue(dltRecord.value(), PaymentChargeFailedEvent.class);
        assertThat(dltEvent.orderId()).isEqualTo(event.orderId());
        assertThat(dltEvent.username()).isEqualTo(event.username());

        assertThrows(Exception.class,
                () -> KafkaTestUtils.getSingleRecord(spyConsumer, "product-commands", Duration.ofSeconds(1)));
    }

    @Test @Order(6)
    void should_PublishToDltAndNotToPaymentCommands_When_DataAccessExceptionOccursDuringUserBalanceDebitFailed() {
        // Arrange
        OrderEntity order = orderRepository.save(OrderEntity.builder()
                .userId(UUID.randomUUID())
                .productId(UUID.randomUUID())
                .quantity(5)
                .status(OrderStatus.CREATED)
                .build());

        UserBalanceDebitFailedEvent event = new UserBalanceDebitFailedEvent(
                order.getId(),
                "user@test.com"
        );
        String topic = env.getRequiredProperty("errors-successful-events.topic.name");
        String messageId = UUID.randomUUID().toString();

        doThrow(new OptimisticLockingFailureException("Database crash on history log"))
                .when(orderHistoryRepository).save(any(OrderHistoryEntity.class));

        // Act
        sendMessage(topic, event.orderId().toString(), event, messageId);

        ConsumerRecord<String, String> dltRecord =
                KafkaTestUtils.getSingleRecord(spyConsumer, "order-ms-dlt", Duration.ofSeconds(5));

        // Assert
        UserBalanceDebitFailedEvent dltEvent = mapper.readValue(dltRecord.value(), UserBalanceDebitFailedEvent.class);
        assertThat(dltEvent.orderId()).isEqualTo(event.orderId());
        assertThat(dltEvent.username()).isEqualTo(event.username());

        assertThrows(Exception.class,
                () -> KafkaTestUtils.getSingleRecord(spyConsumer, "payment-commands", Duration.ofSeconds(1)));
    }

    @Test @Order(8)
    void should_PublishToDltAndNotToUserCommands_When_DataAccessExceptionOccursDuringOrderCompletionFailed() {
        // Arrange
        OrderEntity order = orderRepository.save(OrderEntity.builder()
                .userId(UUID.randomUUID())
                .productId(UUID.randomUUID())
                .quantity(5)
                .status(OrderStatus.CREATED)
                .build());

        OrderCompletionFailedEvent event = new OrderCompletionFailedEvent(
                order.getId(),
                UUID.randomUUID(),
                "user@test.com",
                BigDecimal.TEN);
        String topic = env.getRequiredProperty("errors-successful-events.topic.name");
        String messageId = UUID.randomUUID().toString();

        doThrow(new OptimisticLockingFailureException("Database crash on history log"))
                .when(orderHistoryRepository).save(any(OrderHistoryEntity.class));

        // Act
        sendMessage(topic, event.orderId().toString(), event, messageId);

        ConsumerRecord<String, String> dltRecord =
                KafkaTestUtils.getSingleRecord(spyConsumer, "order-ms-dlt", Duration.ofSeconds(5));

        // Assert
        OrderCompletionFailedEvent dltEvent = mapper.readValue(dltRecord.value(), OrderCompletionFailedEvent.class);
        assertThat(dltEvent.orderId()).isEqualTo(event.orderId());
        assertThat(dltEvent.username()).isEqualTo(event.username());

        assertThrows(Exception.class,
                () -> KafkaTestUtils.getSingleRecord(spyConsumer, "user-commands", Duration.ofSeconds(1)));
    }
}
