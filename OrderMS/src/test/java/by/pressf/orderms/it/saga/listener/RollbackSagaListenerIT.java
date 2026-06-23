package by.pressf.orderms.it.saga.listener;

import by.pressf.core.dto.orchestration.events.order.OrderRejectedEvent;
import by.pressf.core.dto.orchestration.events.payment.PaymentRefundedEvent;
import by.pressf.core.dto.orchestration.events.product.ProductReservationCanceledEvent;
import by.pressf.core.dto.orchestration.events.user.UserBalanceDebitCanceledEvent;
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
public class RollbackSagaListenerIT extends BaseIT {
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
    void should_HandleOrderRejectedEvent_Successfully() {
        OrderEntity order = orderRepository.save(OrderEntity.builder()
                .userId(UUID.randomUUID())
                .productId(UUID.randomUUID())
                .quantity(5)
                .status(OrderStatus.CREATED)
                .build());

        OrderRejectedEvent event = new OrderRejectedEvent(
                order.getId(),
                "user@test.com"
        );
        String topic = env.getRequiredProperty("compensating-events.topic.name");
        String messageId = UUID.randomUUID().toString();

        sendMessage(topic, event.orderId().toString(), event, messageId);

        ConsumerRecord<String, String> record =
                KafkaTestUtils.getSingleRecord(spyConsumer, "email-notification-commands", Duration.ofSeconds(5));
        assertThat(record.value()).contains(event.orderId().toString());

        List<OrderHistoryEntity> history = orderHistoryRepository.findAll().stream()
                .filter(h -> h.getOrderId().equals(event.orderId()))
                .toList();

//        assertThat(history).hasSize(2);
        assertThat(history.get(0).getStatus()).isEqualTo(OrderHistoryStatus.SUCCESS);
        assertThat(history.get(0).getReason())
                .isEqualTo("OrderMS: the order status has been successfully changed to REJECTED");
        assertThat(history.get(1).getReason()).isEqualTo("OrderSaga: saga has completed its work");
    }

    @Test @Order(3)
    void should_HandleProductReservationCanceledEvent_Successfully() {
        OrderEntity order = orderRepository.save(OrderEntity.builder()
                .userId(UUID.randomUUID())
                .productId(UUID.randomUUID())
                .quantity(5)
                .status(OrderStatus.CREATED)
                .build());

        ProductReservationCanceledEvent event = new ProductReservationCanceledEvent(
                order.getId(),
                "user@test.com"
        );
        String topic = env.getRequiredProperty("compensating-events.topic.name");
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
                .isEqualTo("ProductMS: reservation has been successfully lifted from the product");
    }

    @Test @Order(5)
    void should_HandlePaymentRefundedEvent_Successfully() {
        OrderEntity order = orderRepository.save(OrderEntity.builder()
                .userId(UUID.randomUUID())
                .productId(UUID.randomUUID())
                .quantity(5)
                .status(OrderStatus.CREATED)
                .build());

        PaymentRefundedEvent event = new PaymentRefundedEvent(
                order.getId(),
                "user@test.com"
        );
        String topic = env.getRequiredProperty("compensating-events.topic.name");
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
                .isEqualTo("PaymentMS: money has been successfully refunded from the payment");
    }

    @Test @Order(7)
    void should_HandleUserBalanceDebitCanceledEvent_Successfully() {
        OrderEntity order = orderRepository.save(OrderEntity.builder()
                .userId(UUID.randomUUID())
                .productId(UUID.randomUUID())
                .quantity(5)
                .status(OrderStatus.CREATED)
                .build());

        UserBalanceDebitCanceledEvent event = new UserBalanceDebitCanceledEvent(
                order.getId(),
                "user@test.com"
        );
        String topic = env.getRequiredProperty("compensating-events.topic.name");
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
                .isEqualTo("UserMS: the money was successfully refunded to the user");
    }

    @Test @Order(2)
    void should_PublishToDltAndNotToEmailNotificationCommands_When_DataAccessExceptionOccursDuringOrderRejected() {
        // Arrange
        OrderEntity order = orderRepository.save(OrderEntity.builder()
                .userId(UUID.randomUUID())
                .productId(UUID.randomUUID())
                .quantity(5)
                .status(OrderStatus.CREATED)
                .build());

        OrderRejectedEvent event = new OrderRejectedEvent(
                order.getId(),
                "user@test.com"
        );
        String topic = env.getRequiredProperty("compensating-events.topic.name");
        String messageId = UUID.randomUUID().toString();

        doThrow(new OptimisticLockingFailureException("Database rollback log failed"))
                .when(orderHistoryRepository).save(any(OrderHistoryEntity.class));

        // Act
        sendMessage(topic, event.orderId().toString(), event, messageId);

        ConsumerRecord<String, String> dltRecord =
                KafkaTestUtils.getSingleRecord(spyConsumer, "order-ms-dlt", Duration.ofSeconds(5));

        // Assert
        OrderRejectedEvent dltEvent = mapper.readValue(dltRecord.value(), OrderRejectedEvent.class);
        assertThat(dltEvent.orderId()).isEqualTo(event.orderId());
        assertThat(dltEvent.username()).isEqualTo(event.username());

        assertThrows(Exception.class,
                () -> KafkaTestUtils.getSingleRecord(spyConsumer, "email-notification-commands", Duration.ofSeconds(1)));
    }

    @Test @Order(4)
    void should_PublishToDltAndNotToOrderCommands_When_DataAccessExceptionOccursDuringProductReservationCanceled() {
        // Arrange
        OrderEntity order = orderRepository.save(OrderEntity.builder()
                .userId(UUID.randomUUID())
                .productId(UUID.randomUUID())
                .quantity(5)
                .status(OrderStatus.CREATED)
                .build());

        ProductReservationCanceledEvent event = new ProductReservationCanceledEvent(
                order.getId(),
                "user@test.com"
        );
        String topic = env.getRequiredProperty("compensating-events.topic.name");
        String messageId = UUID.randomUUID().toString();

        doThrow(new OptimisticLockingFailureException("Database rollback log failed"))
                .when(orderHistoryRepository).save(any(OrderHistoryEntity.class));

        // Act
        sendMessage(topic, event.orderId().toString(), event, messageId);

        ConsumerRecord<String, String> dltRecord =
                KafkaTestUtils.getSingleRecord(spyConsumer, "order-ms-dlt", Duration.ofSeconds(5));

        // Assert
        ProductReservationCanceledEvent dltEvent =
                mapper.readValue(dltRecord.value(), ProductReservationCanceledEvent.class);
        assertThat(dltEvent.orderId()).isEqualTo(event.orderId());
        assertThat(dltEvent.username()).isEqualTo(event.username());

        assertThrows(Exception.class,
                () -> KafkaTestUtils.getSingleRecord(spyConsumer, "order-commands", Duration.ofSeconds(1)));
    }

    @Test @Order(6)
    void should_PublishToDltAndNotToProductCommands_When_DataAccessExceptionOccursDuringPaymentRefunded() {
        // Arrange
        OrderEntity order = orderRepository.save(OrderEntity.builder()
                .userId(UUID.randomUUID())
                .productId(UUID.randomUUID())
                .quantity(5)
                .status(OrderStatus.CREATED)
                .build());

        PaymentRefundedEvent event = new PaymentRefundedEvent(
                order.getId(),
                "user@test.com"
        );
        String topic = env.getRequiredProperty("compensating-events.topic.name");
        String messageId = UUID.randomUUID().toString();

        doThrow(new OptimisticLockingFailureException("Database rollback log failed"))
                .when(orderHistoryRepository).save(any(OrderHistoryEntity.class));

        // Act
        sendMessage(topic, event.orderId().toString(), event, messageId);

        ConsumerRecord<String, String> dltRecord =
                KafkaTestUtils.getSingleRecord(spyConsumer, "order-ms-dlt", Duration.ofSeconds(5));

        // Assert
        PaymentRefundedEvent dltEvent = mapper.readValue(dltRecord.value(), PaymentRefundedEvent.class);
        assertThat(dltEvent.orderId()).isEqualTo(event.orderId());
        assertThat(dltEvent.username()).isEqualTo(event.username());

        assertThrows(Exception.class,
                () -> KafkaTestUtils.getSingleRecord(spyConsumer, "product-commands", Duration.ofSeconds(1)));
    }

    @Test @Order(8)
    void should_PublishToDltAndNotToPaymentCommands_When_DataAccessExceptionOccursDuringUserBalanceDebitCanceled() {
        // Arrange
        OrderEntity order = orderRepository.save(OrderEntity.builder()
                .userId(UUID.randomUUID())
                .productId(UUID.randomUUID())
                .quantity(5)
                .status(OrderStatus.CREATED)
                .build());

        UserBalanceDebitCanceledEvent event = new UserBalanceDebitCanceledEvent(
                order.getId(),
                "user@test.com"
        );
        String topic = env.getRequiredProperty("compensating-events.topic.name");
        String messageId = UUID.randomUUID().toString();

        doThrow(new OptimisticLockingFailureException("Database rollback log failed"))
                .when(orderHistoryRepository).save(any(OrderHistoryEntity.class));

        // Act
        sendMessage(topic, event.orderId().toString(), event, messageId);

        ConsumerRecord<String, String> dltRecord =
                KafkaTestUtils.getSingleRecord(spyConsumer, "order-ms-dlt", Duration.ofSeconds(5));

        // Assert
        UserBalanceDebitCanceledEvent dltEvent =
                mapper.readValue(dltRecord.value(), UserBalanceDebitCanceledEvent.class);
        assertThat(dltEvent.orderId()).isEqualTo(event.orderId());
        assertThat(dltEvent.username()).isEqualTo(event.username());

        assertThrows(Exception.class,
                () -> KafkaTestUtils.getSingleRecord(spyConsumer, "payment-commands", Duration.ofSeconds(1)));
    }
}
