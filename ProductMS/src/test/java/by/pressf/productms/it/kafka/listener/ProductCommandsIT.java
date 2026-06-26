package by.pressf.productms.it.kafka.listener;

import by.pressf.core.dto.orchestration.commands.product.CancelProductReservationCommand;
import by.pressf.core.dto.orchestration.commands.product.ReserveProductCommand;
import by.pressf.core.dto.orchestration.events.product.ProductReservationCancelFailedEvent;
import by.pressf.core.dto.orchestration.events.product.ProductReservationCanceledEvent;
import by.pressf.core.dto.orchestration.events.product.ProductReservationFailedEvent;
import by.pressf.core.dto.orchestration.events.product.ProductReservedEvent;
import by.pressf.productms.dao.entity.EventEntity;
import by.pressf.productms.dao.entity.ProductEntity;
import by.pressf.productms.dao.entity.ProductHistoryEntity;
import by.pressf.productms.dao.entity.status.ProductStatus;
import by.pressf.productms.dao.repository.EventRepository;
import by.pressf.productms.dao.repository.ProductHistoryRepository;
import by.pressf.productms.it.config.BaseIT;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doThrow;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ProductCommandsIT extends BaseIT {
    @Autowired
    private ProductHistoryRepository productHistoryRepository;
    @Autowired
    private EventRepository eventRepository;

    @BeforeEach
    void setUp() {
        spyConsumer.poll(Duration.ofMillis(100));
        productHistoryRepository.deleteAll();
        productRepository.deleteAll();
    }

    @Test @Order(1)
    void should_ReserveProductAndPublishSuccessfulEvent_When_ReserveProductCommandIsValid() {
        // Arrange
        ProductEntity product = productRepository.save(ProductEntity.builder()
                .name("Laptop")
                .quantity(10)
                .price(new BigDecimal("1000.00"))
                .version(0)
                .build());

        ReserveProductCommand command = new ReserveProductCommand(
                UUID.randomUUID(),
                product.getId(),
                UUID.randomUUID(),
                "john_doe",
                2
        );

        String topic = env.getRequiredProperty("successful-events.topic.name");

        // Act
        sendMessage(env.getRequiredProperty("product.commands.topic.name"), command.orderId().toString(),
                command, UUID.randomUUID().toString());

        ConsumerRecord<String, String> record =
                KafkaTestUtils.getSingleRecord(spyConsumer, topic, Duration.ofSeconds(5));

        // Assert
        assertThat(record.key()).isEqualTo(command.orderId().toString());
        assertThat(record.value()).isNotNull();
        assertThat(record.headers().lastHeader("messageId")).isNotNull();

        ProductReservedEvent event = mapper.readValue(record.value(), ProductReservedEvent.class);
        assertThat(event.orderId()).isEqualTo(command.orderId());
        assertThat(event.userId()).isEqualTo(command.userId());
        assertThat(event.username()).isEqualTo(command.username());
        assertThat(event.amount()).isEqualByComparingTo(new BigDecimal("2000.00"));

        product = productRepository.findById(product.getId())
                .orElseThrow(() -> new AssertionError("Товар не найден в БД"));
        assertThat(product.getQuantity()).isEqualTo(8);

        ProductHistoryEntity history = productHistoryRepository.findByOrderId(command.orderId());
        assertThat(history).isNotNull();
        assertThat(history.getStatus()).isEqualTo(ProductStatus.RESERVED);
        assertThat(history.getQuantity()).isEqualTo(2);
    }

    @Test @Order(1)
    void should_NotPublishToSuccessfulTopic_When_DuplicateMessageReceived() {
        // Arrange
        String messageId = UUID.randomUUID().toString();
        eventRepository.save(EventEntity.builder().messageId(messageId).build());

        ProductEntity product = productRepository.save(ProductEntity.builder()
                .name("Mouse")
                .quantity(10)
                .price(new BigDecimal("25.00"))
                .version(0)
                .build());

        ReserveProductCommand command = new ReserveProductCommand(
                UUID.randomUUID(),
                product.getId(),
                UUID.randomUUID(),
                "john_doe",
                1
        );

        String topic = env.getRequiredProperty("successful-events.topic.name");

        // Act
        sendMessage(env.getRequiredProperty("product.commands.topic.name"), command.orderId().toString(),
                command, messageId);

        // Assert
        assertThrows(Exception.class,
                () -> KafkaTestUtils.getSingleRecord(spyConsumer, topic, Duration.ofSeconds(3)));

        product = productRepository.findById(product.getId())
                .orElseThrow(() -> new AssertionError("Товар не найден в БД"));
        assertThat(product.getQuantity()).isEqualTo(10);
    }

    @Test @Order(2)
    void should_PublishToErrorsSuccessfulTopicAfterRetries_When_OptimisticLockingFailureOccursDuringReservation() {
        // Arrange
        ProductEntity product = productRepository.save(ProductEntity.builder()
                .name("Smartphone")
                .quantity(5)
                .price(new BigDecimal("500.00"))
                .version(0)
                .build());

        doThrow(new OptimisticLockingFailureException("!!!errors!!!")).when(productRepository)
                .save(argThat(p -> "Smartphone".equals(p.getName())));

        String topic = env.getRequiredProperty("errors-successful-events.topic.name");

        ReserveProductCommand command = new ReserveProductCommand(
                UUID.randomUUID(),
                product.getId(),
                UUID.randomUUID(),
                "john_doe",
                1
        );

        // Act
        sendMessage(env.getRequiredProperty("product.commands.topic.name"), command.orderId().toString(),
                command, UUID.randomUUID().toString());

        ConsumerRecord<String, String> record =
                KafkaTestUtils.getSingleRecord(spyConsumer, topic, Duration.ofSeconds(12));

        // Assert
        assertThat(record.key()).isEqualTo(command.orderId().toString());
        assertThat(record.value()).isNotNull();
        assertThat(record.headers().lastHeader("messageId")).isNotNull();

        ProductReservationFailedEvent failedEvent =
                mapper.readValue(record.value(), ProductReservationFailedEvent.class);
        assertThat(failedEvent.orderId()).isEqualTo(command.orderId());
        assertThat(failedEvent.username()).isEqualTo(command.username());

        ProductEntity productCheck = productRepository.findById(product.getId())
                .orElseThrow(() -> new AssertionError("Товар не найден в БД"));
        assertThat(productCheck.getQuantity()).isEqualTo(5);
    }

    @Test @Order(1)
    void should_PublishToErrorsSuccessfulTopicImmediatelyWithoutRetries_When_ProductInsufficientDuringReservation() {
        // Arrange
        ProductEntity product = productRepository.save(ProductEntity.builder()
                .name("Tablet")
                .quantity(1)
                .price(new BigDecimal("300.00"))
                .version(0)
                .build());

        String topic = env.getRequiredProperty("errors-successful-events.topic.name");

        ReserveProductCommand command = new ReserveProductCommand(
                UUID.randomUUID(),
                product.getId(),
                UUID.randomUUID(),
                "john_doe",
                10
        );

        // Act
        sendMessage(env.getRequiredProperty("product.commands.topic.name"), command.orderId().toString(),
                command, UUID.randomUUID().toString());

        ConsumerRecord<String, String> record =
                KafkaTestUtils.getSingleRecord(spyConsumer, topic, Duration.ofSeconds(5));

        // Assert
        assertThat(record.key()).isEqualTo(command.orderId().toString());
        assertThat(record.value()).isNotNull();
        assertThat(record.headers().lastHeader("messageId")).isNotNull();

        ProductReservationFailedEvent failedEvent = mapper.readValue(record.value(), ProductReservationFailedEvent.class);
        assertThat(failedEvent.orderId()).isEqualTo(command.orderId());
        assertThat(failedEvent.username()).isEqualTo(command.username());

        ProductEntity productCheck = productRepository.findById(product.getId())
                .orElseThrow(() -> new AssertionError("Товар не найден в БД"));
        assertThat(productCheck.getQuantity()).isEqualTo(1);
    }

    @Test @Order(1)
    void should_CancelReservationAndPublishCompensatingEvent_When_CancelProductReservationCommandIsValid() {
        // Arrange
        ProductEntity product = productRepository.save(ProductEntity.builder()
                .name("Headphones")
                .quantity(5)
                .price(new BigDecimal("100.00"))
                .version(0)
                .build());

        UUID orderId = UUID.randomUUID();
        ProductHistoryEntity history = productHistoryRepository.save(ProductHistoryEntity.builder()
                .orderId(orderId)
                .product(product)
                .quantity(3)
                .status(ProductStatus.RESERVED)
                .build());

        CancelProductReservationCommand command = new CancelProductReservationCommand(
                orderId,
                "john_doe"
        );

        String topic = env.getRequiredProperty("compensating-events.topic.name");

        // Act
        sendMessage(env.getRequiredProperty("product.commands.topic.name"), command.orderId().toString(),
                command, UUID.randomUUID().toString());

        ConsumerRecord<String, String> record =
                KafkaTestUtils.getSingleRecord(spyConsumer, topic, Duration.ofSeconds(5));

        // Assert
        assertThat(record.key()).isEqualTo(command.orderId().toString());
        assertThat(record.value()).isNotNull();
        assertThat(record.headers().lastHeader("messageId")).isNotNull();

        ProductReservationCanceledEvent event = mapper.readValue(record.value(), ProductReservationCanceledEvent.class);
        assertThat(event.orderId()).isEqualTo(command.orderId());
        assertThat(event.username()).isEqualTo(command.username());

        product = productRepository.findById(product.getId())
                .orElseThrow(() -> new AssertionError("Товар не найден в БД"));
        assertThat(product.getQuantity()).isEqualTo(8);

        history = productHistoryRepository.findById(history.getId())
                .orElseThrow(() -> new AssertionError("История товара не найдена в БД"));
        assertThat(history.getStatus()).isEqualTo(ProductStatus.CANCELLED);
        assertThat(history.getUpdatedAt()).isNotNull();
    }

    @Test @Order(2)
    void should_PublishToErrorsCompensatingTopicAfterRetries_When_OptimisticLockingFailureOccursDuringCancellation() {
        // Arrange
        ProductEntity product = productRepository.save(ProductEntity.builder()
                .name("Keyboard")
                .quantity(5)
                .price(new BigDecimal("50.00"))
                .version(0)
                .build());

        UUID orderId = UUID.randomUUID();
        productHistoryRepository.save(ProductHistoryEntity.builder()
                .orderId(orderId)
                .product(product)
                .quantity(2)
                .status(ProductStatus.RESERVED)
                .build());

        doThrow(new OptimisticLockingFailureException("!!!errors!!!")).when(productRepository)
                .save(argThat(p -> "Keyboard".equals(p.getName())));

        String topic = env.getRequiredProperty("errors-compensating-events.topic.name");

        CancelProductReservationCommand command = new CancelProductReservationCommand(
                orderId,
                "john_doe"
        );

        // Act
        sendMessage(env.getRequiredProperty("product.commands.topic.name"), command.orderId().toString(),
                command, UUID.randomUUID().toString());

        ConsumerRecord<String, String> record =
                KafkaTestUtils.getSingleRecord(spyConsumer, topic, Duration.ofSeconds(12));

        // Assert
        assertThat(record.key()).isEqualTo(command.orderId().toString());
        assertThat(record.value()).isNotNull();
        assertThat(record.headers().lastHeader("messageId")).isNotNull();

        ProductReservationCancelFailedEvent failedEvent = mapper.readValue(record.value(), ProductReservationCancelFailedEvent.class);
        assertThat(failedEvent.orderId()).isEqualTo(command.orderId());
        assertThat(failedEvent.username()).isEqualTo(command.username());
    }

    @Test @Order(1)
    void should_PublishToErrorsCompensatingTopicImmediatelyWithoutRetries_When_ProductHistoryNotFoundDuringCancellation() {
        // Arrange
        String topic = env.getRequiredProperty("errors-compensating-events.topic.name");

        CancelProductReservationCommand command = new CancelProductReservationCommand(
                UUID.randomUUID(),
                "john_doe"
        );

        // Act
        sendMessage(env.getRequiredProperty("product.commands.topic.name"), command.orderId().toString(),
                command, UUID.randomUUID().toString());

        ConsumerRecord<String, String> record =
                KafkaTestUtils.getSingleRecord(spyConsumer, topic, Duration.ofSeconds(5));

        // Assert
        assertThat(record.key()).isEqualTo(command.orderId().toString());
        assertThat(record.value()).isNotNull();
        assertThat(record.headers().lastHeader("messageId")).isNotNull();

        ProductReservationCancelFailedEvent failedEvent = mapper.readValue(record.value(), ProductReservationCancelFailedEvent.class);
        assertThat(failedEvent.orderId()).isEqualTo(command.orderId());
        assertThat(failedEvent.username()).isEqualTo(command.username());
    }
}
