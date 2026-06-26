package by.pressf.orderms.it.kafka.listener;

import by.pressf.core.dto.orchestration.commands.order.ConfirmOrderCommand;
import by.pressf.core.dto.orchestration.commands.order.RejectOrderCommand;
import by.pressf.core.dto.orchestration.events.order.OrderCompletedEvent;
import by.pressf.core.dto.orchestration.events.order.OrderCompletionFailedEvent;
import by.pressf.core.dto.orchestration.events.order.OrderRejectedEvent;
import by.pressf.core.dto.orchestration.events.order.OrderRejectionFailedEvent;
import by.pressf.orderms.dao.entity.OrderEntity;
import by.pressf.orderms.dao.entity.status.OrderStatus;
import by.pressf.orderms.it.config.BaseIT;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class OrderCommandsIT extends BaseIT {
    @BeforeAll
    static void init() {
        spyConsumer = createSpyConsumer(List.of("successful-events", "errors-successful-events",
                "compensating-events", "errors-compensating-events"));
    }

    @BeforeEach
    void setUp() {
        spyConsumer.poll(Duration.ofMillis(100));
        orderRepository.deleteAll();
    }

    @AfterAll
    static void destruct() { spyConsumer.close(); }

    @Test
    void should_ApproveOrderAndPublishCompletedEvent_When_ConfirmCommandIsValid() {
        // Arrange
        OrderEntity order = orderRepository.save(OrderEntity.builder()
                .userId(UUID.randomUUID())
                .productId(UUID.randomUUID())
                .quantity(2)
                .status(OrderStatus.CREATED)
                .build());

        ConfirmOrderCommand command = new ConfirmOrderCommand(
                order.getId(),
                order.getUserId(),
                "Danny_Black",
                new BigDecimal("250.00")
        );

        String orderCommandsTopic = env.getRequiredProperty("order.commands.topic.name");
        String successfulEventsTopic = env.getRequiredProperty("successful-events.topic.name");
        String messageId = UUID.randomUUID().toString();

        // Act
        sendMessage(orderCommandsTopic, command.orderId().toString(), command, messageId);

        ConsumerRecord<String, String> record =
                KafkaTestUtils.getSingleRecord(spyConsumer, successfulEventsTopic, Duration.ofSeconds(5));

        // Assert
        assertThat(record.key()).isEqualTo(command.orderId().toString());
        assertThat(record.value()).isNotNull();
        assertThat(record.headers().lastHeader("messageId")).isNotNull();

        OrderCompletedEvent event = mapper.readValue(record.value(), OrderCompletedEvent.class);
        assertThat(event.orderId()).isEqualTo(command.orderId());
        assertThat(event.username()).isEqualTo(command.username());

        order = orderRepository.findById(order.getId())
                .orElseThrow(() -> new AssertionError("Заказ не найден в БД"));
        assertThat(order.getStatus()).isEqualTo(OrderStatus.APPROVED);
    }

    @Test
    void should_PublishToErrorsSuccessfulTopic_When_OrderNotFoundDuringConfirm() {
        // Arrange
        ConfirmOrderCommand command = new ConfirmOrderCommand(
                UUID.randomUUID(),
                UUID.randomUUID(),
                "Danny_Black",
                new BigDecimal("250.00")
        );

        String orderCommandsTopic = env.getRequiredProperty("order.commands.topic.name");
        String errorsSuccessfulTopic = env.getRequiredProperty("errors-successful-events.topic.name");

        // Act
        sendMessage(orderCommandsTopic, command.orderId().toString(), command, UUID.randomUUID().toString());

        ConsumerRecord<String, String> record =
                KafkaTestUtils.getSingleRecord(spyConsumer, errorsSuccessfulTopic, Duration.ofSeconds(5));

        // Assert
        assertThat(record.key()).isEqualTo(command.orderId().toString());
        assertThat(record.value()).isNotNull();
        assertThat(record.headers().lastHeader("messageId")).isNotNull();

        OrderCompletionFailedEvent failedEvent =
                mapper.readValue(record.value(), OrderCompletionFailedEvent.class);
        assertThat(failedEvent.orderId()).isEqualTo(command.orderId());
        assertThat(failedEvent.userId()).isEqualTo(command.userId());
        assertThat(failedEvent.username()).isEqualTo(command.username());
        assertThat(failedEvent.amount()).isEqualByComparingTo(command.amount());
    }

    @Test
    void should_RejectOrderAndPublishRejectedEvent_When_RejectCommandIsValid() {
        // Arrange
        OrderEntity order = orderRepository.save(OrderEntity.builder()
                .userId(UUID.randomUUID())
                .productId(UUID.randomUUID())
                .quantity(1)
                .status(OrderStatus.CREATED)
                .build());

        RejectOrderCommand command = new RejectOrderCommand(
                order.getId(),
                "Danny_Black"
        );

        String orderCommandsTopic = env.getRequiredProperty("order.commands.topic.name");
        String compensatingEventsTopic = env.getRequiredProperty("compensating-events.topic.name");
        String messageId = UUID.randomUUID().toString();

        // Act
        sendMessage(orderCommandsTopic, command.orderId().toString(), command, messageId);

        ConsumerRecord<String, String> record =
                KafkaTestUtils.getSingleRecord(spyConsumer, compensatingEventsTopic, Duration.ofSeconds(5));

        // Assert
        assertThat(record.key()).isEqualTo(command.orderId().toString());
        assertThat(record.value()).isNotNull();
        assertThat(record.headers().lastHeader("messageId")).isNotNull();

        OrderRejectedEvent event = mapper.readValue(record.value(), OrderRejectedEvent.class);
        assertThat(event.orderId()).isEqualTo(command.orderId());
        assertThat(event.username()).isEqualTo(command.username());

        order = orderRepository.findById(order.getId())
                .orElseThrow(() -> new AssertionError("Заказ не найден в БД"));
        assertThat(order.getStatus()).isEqualTo(OrderStatus.REJECTED);
    }

    @Test
    void should_PublishToErrorsCompensatingTopic_When_OrderNotFoundDuringReject() {
        // Arrange
        RejectOrderCommand command = new RejectOrderCommand(
                UUID.randomUUID(),
                "Danny_Black"
        );

        String orderCommandsTopic = env.getRequiredProperty("order.commands.topic.name");
        String errorsCompensatingTopic = env.getRequiredProperty("errors-compensating-events.topic.name");

        // Act
        sendMessage(orderCommandsTopic, command.orderId().toString(), command, UUID.randomUUID().toString());

        ConsumerRecord<String, String> record =
                KafkaTestUtils.getSingleRecord(spyConsumer, errorsCompensatingTopic, Duration.ofSeconds(5));

        // Assert
        assertThat(record.key()).isEqualTo(command.orderId().toString());
        assertThat(record.value()).isNotNull();
        assertThat(record.headers().lastHeader("messageId")).isNotNull();

        OrderRejectionFailedEvent failedEvent =
                mapper.readValue(record.value(), OrderRejectionFailedEvent.class);
        assertThat(failedEvent.orderId()).isEqualTo(command.orderId());
        assertThat(failedEvent.username()).isEqualTo(command.username());
    }
}
