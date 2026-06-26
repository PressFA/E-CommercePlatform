package by.pressf.orderms.it.saga.listener;

import by.pressf.core.dto.orchestration.events.order.OrderRejectionFailedEvent;
import by.pressf.core.dto.orchestration.events.payment.PaymentRefundFailedEvent;
import by.pressf.core.dto.orchestration.events.product.ProductReservationCancelFailedEvent;
import by.pressf.core.dto.orchestration.events.user.UserBalanceDebitCancelFailedEvent;
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

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class CriticalAuditSagaListenerIT extends BaseIT {
    @Autowired
    private OrderHistoryRepository orderHistoryRepository;

    @BeforeAll
    static void init() {
        spyConsumer = createSpyConsumer(List.of("email-notification-commands", "order-commands", "product-commands",
                "payment-commands", "order-ms-dlt"));
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
    void should_HandleOrderRejectionFailedEvent_Successfully() {
        OrderEntity order = orderRepository.save(OrderEntity.builder()
                .userId(UUID.randomUUID())
                .productId(UUID.randomUUID())
                .quantity(5)
                .status(OrderStatus.CREATED)
                .build());

        OrderRejectionFailedEvent event = new OrderRejectionFailedEvent(
                order.getId(),
                "user@test.com"
        );
        String topic = env.getRequiredProperty("errors-compensating-events.topic.name");
        String messageId = UUID.randomUUID().toString();

        sendMessage(topic, event.orderId().toString(), event, messageId);

        ConsumerRecord<String, String> record =
                KafkaTestUtils.getSingleRecord(spyConsumer, "email-notification-commands", Duration.ofSeconds(5));
        assertThat(record.value()).contains(event.orderId().toString());

        List<OrderHistoryEntity> history = orderHistoryRepository.findAll().stream()
                .filter(h -> h.getOrderId().equals(event.orderId()))
                .toList();

//        assertThat(history).hasSize(1);
        assertThat(history.getFirst().getStatus()).isEqualTo(OrderHistoryStatus.FAIL);
        assertThat(history.getFirst().getReason())
                .isEqualTo("OrderMS(Warning!!!): an error occurred while compensating for a transaction; for more information, see the logs.");
    }

    @Test @Order(3)
    void should_HandleProductReservationCancelFailedEvent_Successfully() {
        OrderEntity order = orderRepository.save(OrderEntity.builder()
                .userId(UUID.randomUUID())
                .productId(UUID.randomUUID())
                .quantity(5)
                .status(OrderStatus.CREATED)
                .build());

        ProductReservationCancelFailedEvent event = new ProductReservationCancelFailedEvent(
                order.getId(),
                "user@test.com"
        );
        String topic = env.getRequiredProperty("errors-compensating-events.topic.name");
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
                .isEqualTo("ProductMS(Warning!!!): an error occurred while compensating for a transaction; for more information, see the logs.");
    }

    @Test @Order(5)
    void should_HandlePaymentRefundFailedEvent_Successfully() {
        OrderEntity order = orderRepository.save(OrderEntity.builder()
                .userId(UUID.randomUUID())
                .productId(UUID.randomUUID())
                .quantity(5)
                .status(OrderStatus.CREATED)
                .build());

        PaymentRefundFailedEvent event = new PaymentRefundFailedEvent(
                order.getId(),
                "user@test.com"
        );
        String topic = env.getRequiredProperty("errors-compensating-events.topic.name");
        String messageId = UUID.randomUUID().toString();

        sendMessage(topic, event.orderId().toString(), event, messageId);

        ConsumerRecord<String, String> record =
                KafkaTestUtils.getSingleRecord(spyConsumer, "product-commands", Duration.ofSeconds(5));
        assertThat(record.value()).contains(event.orderId().toString());

        List<OrderHistoryEntity> history = orderHistoryRepository.findAll().stream()
                .filter(h -> h.getOrderId().equals(event.orderId()))
                .toList();

//        assertThat(history).hasSize(1);
        assertThat(history.getFirst().getReason())
                .isEqualTo("PaymentMS(Warning!!!): an error occurred while compensating for a transaction; for more information, see the logs.");
    }

    @Test @Order(7)
    void should_HandleUserBalanceDebitCancelFailedEvent_Successfully() {
        OrderEntity order = orderRepository.save(OrderEntity.builder()
                .userId(UUID.randomUUID())
                .productId(UUID.randomUUID())
                .quantity(5)
                .status(OrderStatus.CREATED)
                .build());

        UserBalanceDebitCancelFailedEvent event = new UserBalanceDebitCancelFailedEvent(
                order.getId(),
                "user@test.com"
        );
        String topic = env.getRequiredProperty("errors-compensating-events.topic.name");
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
                .isEqualTo("UserMS(Warning!!!): an error occurred while compensating for a transaction; for more information, see the logs.");
    }

    @Test @Order(2)
    void should_PublishToDltAndNotToEmailCommands_When_DataAccessExceptionOccursDuringOrderRejectionFailed() {
        // Arrange
        OrderEntity order = orderRepository.save(OrderEntity.builder()
                .userId(UUID.randomUUID())
                .productId(UUID.randomUUID())
                .quantity(5)
                .status(OrderStatus.CREATED)
                .build());

        OrderRejectionFailedEvent event = new OrderRejectionFailedEvent(
                order.getId(),
                "user@test.com"
        );
        String topic = env.getRequiredProperty("errors-compensating-events.topic.name");
        String messageId = UUID.randomUUID().toString();

        doThrow(new OptimisticLockingFailureException("DB error during audit"))
                .when(orderHistoryRepository).save(any(OrderHistoryEntity.class));

        // Act
        sendMessage(topic, event.orderId().toString(), event, messageId);

        ConsumerRecord<String, String> dltRecord =
                KafkaTestUtils.getSingleRecord(spyConsumer, "order-ms-dlt", Duration.ofSeconds(5));

        // Assert
        OrderRejectionFailedEvent dltEvent = mapper.readValue(dltRecord.value(), OrderRejectionFailedEvent.class);
        assertThat(dltEvent.orderId()).isEqualTo(event.orderId());
        assertThat(dltEvent.username()).isEqualTo(event.username());

        assertThrows(Exception.class,
                () -> KafkaTestUtils.getSingleRecord(spyConsumer, "email-notification-commands", Duration.ofSeconds(1)));
    }

    @Test @Order(4)
    void should_PublishToDltAndNotToOrderCommands_When_DataAccessExceptionOccursDuringProductReservationCancelFailed() {
        // Arrange
        OrderEntity order = orderRepository.save(OrderEntity.builder()
                .userId(UUID.randomUUID())
                .productId(UUID.randomUUID())
                .quantity(5)
                .status(OrderStatus.CREATED)
                .build());

        ProductReservationCancelFailedEvent event = new ProductReservationCancelFailedEvent(
                order.getId(),
                "user@test.com"
        );
        String topic = env.getRequiredProperty("errors-compensating-events.topic.name");
        String messageId = UUID.randomUUID().toString();

        doThrow(new OptimisticLockingFailureException("DB error during audit"))
                .when(orderHistoryRepository).save(any(OrderHistoryEntity.class));

        // Act
        sendMessage(topic, event.orderId().toString(), event, messageId);

        ConsumerRecord<String, String> dltRecord =
                KafkaTestUtils.getSingleRecord(spyConsumer, "order-ms-dlt", Duration.ofSeconds(5));

        // Assert
        ProductReservationCancelFailedEvent dltEvent =
                mapper.readValue(dltRecord.value(), ProductReservationCancelFailedEvent.class);
        assertThat(dltEvent.orderId()).isEqualTo(event.orderId());
        assertThat(dltEvent.username()).isEqualTo(event.username());

        assertThrows(Exception.class,
                () -> KafkaTestUtils.getSingleRecord(spyConsumer, "order-commands", Duration.ofSeconds(1)));
    }

    @Test @Order(6)
    void should_PublishToDltAndNotToProductCommands_When_DataAccessExceptionOccursDuringPaymentRefundFailed() {
        // Arrange
        OrderEntity order = orderRepository.save(OrderEntity.builder()
                .userId(UUID.randomUUID())
                .productId(UUID.randomUUID())
                .quantity(5)
                .status(OrderStatus.CREATED)
                .build());

        PaymentRefundFailedEvent event = new PaymentRefundFailedEvent(
                order.getId(),
                "user@test.com"
        );
        String topic = env.getRequiredProperty("errors-compensating-events.topic.name");
        String messageId = UUID.randomUUID().toString();

        doThrow(new OptimisticLockingFailureException("DB error during audit"))
                .when(orderHistoryRepository).save(any(OrderHistoryEntity.class));

        // Act
        sendMessage(topic, event.orderId().toString(), event, messageId);

        ConsumerRecord<String, String> dltRecord =
                KafkaTestUtils.getSingleRecord(spyConsumer, "order-ms-dlt", Duration.ofSeconds(5));

        // Assert
        PaymentRefundFailedEvent dltEvent = mapper.readValue(dltRecord.value(), PaymentRefundFailedEvent.class);
        assertThat(dltEvent.orderId()).isEqualTo(event.orderId());
        assertThat(dltEvent.username()).isEqualTo(event.username());

        assertThrows(Exception.class,
                () -> KafkaTestUtils.getSingleRecord(spyConsumer, "product-commands", Duration.ofSeconds(1)));
    }

    @Test @Order(8)
    void should_PublishToDltAndNotToPaymentCommands_When_DataAccessExceptionOccursDuringUserBalanceDebitCancelFailed() {
        // Arrange
        OrderEntity order = orderRepository.save(OrderEntity.builder()
                .userId(UUID.randomUUID())
                .productId(UUID.randomUUID())
                .quantity(5)
                .status(OrderStatus.CREATED)
                .build());

        UserBalanceDebitCancelFailedEvent event = new UserBalanceDebitCancelFailedEvent(
                order.getId(),
                "user@test.com"
        );
        String topic = env.getRequiredProperty("errors-compensating-events.topic.name");
        String messageId = UUID.randomUUID().toString();

        doThrow(new OptimisticLockingFailureException("DB error during audit"))
                .when(orderHistoryRepository).save(any(OrderHistoryEntity.class));

        // Act
        sendMessage(topic, event.orderId().toString(), event, messageId);

        ConsumerRecord<String, String> dltRecord =
                KafkaTestUtils.getSingleRecord(spyConsumer, "order-ms-dlt", Duration.ofSeconds(5));

        // Assert
        UserBalanceDebitCancelFailedEvent dltEvent =
                mapper.readValue(dltRecord.value(), UserBalanceDebitCancelFailedEvent.class);
        assertThat(dltEvent.orderId()).isEqualTo(event.orderId());
        assertThat(dltEvent.username()).isEqualTo(event.username());

        assertThrows(Exception.class,
                () -> KafkaTestUtils.getSingleRecord(spyConsumer, "payment-commands", Duration.ofSeconds(1)));
    }
}
