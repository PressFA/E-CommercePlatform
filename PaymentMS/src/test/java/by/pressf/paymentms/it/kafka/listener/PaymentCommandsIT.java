package by.pressf.paymentms.it.kafka.listener;

import by.pressf.core.dto.orchestration.commands.payment.ChargePaymentCommand;
import by.pressf.core.dto.orchestration.commands.payment.RefundPaymentCommand;
import by.pressf.core.dto.orchestration.events.payment.PaymentChargeFailedEvent;
import by.pressf.core.dto.orchestration.events.payment.PaymentChargedEvent;
import by.pressf.core.dto.orchestration.events.payment.PaymentRefundFailedEvent;
import by.pressf.core.dto.orchestration.events.payment.PaymentRefundedEvent;
import by.pressf.paymentms.dao.entity.EventEntity;
import by.pressf.paymentms.dao.entity.PaymentEntity;
import by.pressf.paymentms.dao.entity.type.PaymentType;
import by.pressf.paymentms.dao.repository.EventRepository;
import by.pressf.paymentms.exception.PaymentFailedException;
import by.pressf.paymentms.it.config.BaseIT;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class PaymentCommandsIT extends BaseIT {
    @Autowired
    private EventRepository eventRepository;

    @BeforeAll
    static void init() {
        spyConsumer = createSpyConsumer(List.of("successful-events", "errors-successful-events",
                "compensating-events", "errors-compensating-events"));
    }

    @BeforeEach
    void setUp() {
        spyConsumer.poll(Duration.ofMillis(100));
        paymentRepository.deleteAll();
    }

    @AfterAll
    static void destruct() { spyConsumer.close(); }

    @Test
    @Order(1)
    void should_ChargePaymentSuccessfully_When_ChargePaymentCommandReceived() throws Exception {
        // Arrange
        UUID orderId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        String username = "john_doe";
        BigDecimal amount = new BigDecimal("150.00");
        String messageId = UUID.randomUUID().toString();

        ChargePaymentCommand command = new ChargePaymentCommand(
                orderId,
                userId,
                username,
                amount
        );
        when(stripeService.createPayment(any())).thenReturn("ch_stripe_ok_123");

        // Act
        sendMessage(env.getRequiredProperty("payment.commands.topic.name"),
                orderId.toString(), command, messageId);

        // Assert
        ConsumerRecord<String, String> record =
                KafkaTestUtils.getSingleRecord(spyConsumer, "successful-events", Duration.ofSeconds(5));
        assertThat(record.key()).isEqualTo(orderId.toString());

        PaymentChargedEvent event = mapper.readValue(record.value(), PaymentChargedEvent.class);
        assertThat(event.orderId()).isEqualTo(orderId);
        assertThat(event.userId()).isEqualTo(userId);
        assertThat(event.username()).isEqualTo(username);
        assertThat(event.amount()).isEqualByComparingTo(amount);

        PaymentEntity savedPayment = paymentRepository.findByOrderId(orderId);
        assertThat(savedPayment).isNotNull();
        assertThat(savedPayment.getType()).isEqualTo(PaymentType.PAYMENT);
        assertThat(savedPayment.getStripeId()).isEqualTo("ch_stripe_ok_123");
    }

    @Test
    @Order(3)
    void should_PublishToErrorsSuccessfulEventsWithRetryableException_When_ChargePaymentFailsWithRetryableStatus() throws Exception {
        // Arrange
        UUID orderId = UUID.randomUUID();
        ChargePaymentCommand command = new ChargePaymentCommand(
                orderId,
                UUID.randomUUID(),
                "user",
                new BigDecimal("50.00")
        );
        String messageId = UUID.randomUUID().toString();

        when(stripeService.createPayment(any()))
                .thenThrow(new PaymentFailedException("Stripe temporary error", 402));

        // Act
        sendMessage(env.getRequiredProperty("payment.commands.topic.name"),
                orderId.toString(), command, messageId);

        // Assert
        ConsumerRecord<String, String> record =
                KafkaTestUtils.getSingleRecord(spyConsumer, "errors-successful-events", Duration.ofSeconds(12));
        assertThat(record.key()).isEqualTo(orderId.toString());

        PaymentChargeFailedEvent failedEvent = mapper.readValue(record.value(), PaymentChargeFailedEvent.class);
        assertThat(failedEvent.orderId()).isEqualTo(orderId);
        assertThat(failedEvent.username()).isEqualTo("user");
        assertThat(paymentRepository.findAll()).isEmpty();
    }

    @Test
    @Order(1)
    void should_PublishToErrorsSuccessfulEventsWithNotRetryableException_When_ChargePaymentFailsWithNotRetryableStatus() throws Exception {
        // Arrange
        UUID orderId = UUID.randomUUID();
        ChargePaymentCommand command = new ChargePaymentCommand(
                orderId, UUID.randomUUID(),
                "user",
                new BigDecimal("50.00")
        );
        String messageId = UUID.randomUUID().toString();

        when(stripeService.createPayment(any()))
                .thenThrow(new PaymentFailedException("Stripe fatal error", 500));

        // Act
        sendMessage(env.getRequiredProperty("payment.commands.topic.name"),
                orderId.toString(), command, messageId);

        // Assert
        ConsumerRecord<String, String> record =
                KafkaTestUtils.getSingleRecord(spyConsumer, "errors-successful-events", Duration.ofSeconds(5));
        assertThat(record.key()).isEqualTo(orderId.toString());

        PaymentChargeFailedEvent failedEvent = mapper.readValue(record.value(), PaymentChargeFailedEvent.class);
        assertThat(failedEvent.orderId()).isEqualTo(orderId);
        assertThat(paymentRepository.findAll()).isEmpty();
    }

    @Test
    @Order(1)
    void should_RefundPaymentSuccessfully_When_RefundPaymentCommandReceived() throws Exception {
        // Arrange
        UUID orderId = UUID.randomUUID();
        paymentRepository.save(PaymentEntity.builder()
                .userId(UUID.randomUUID())
                .orderId(orderId)
                .stripeId("ch_stripe_123")
                .amount(new BigDecimal("100.00"))
                .type(PaymentType.PAYMENT)
                .build());

        RefundPaymentCommand command = new RefundPaymentCommand(orderId, "john_doe");
        String messageId = UUID.randomUUID().toString();
        when(stripeService.createRefundPayment(any())).thenReturn("re_stripe_ok_123");

        // Act
        sendMessage(env.getRequiredProperty("payment.commands.topic.name"), orderId.toString(), command, messageId);

        // Assert
        ConsumerRecord<String, String> record =
                KafkaTestUtils.getSingleRecord(spyConsumer, "compensating-events", Duration.ofSeconds(5));
        assertThat(record.key()).isEqualTo(orderId.toString());

        PaymentRefundedEvent event = mapper.readValue(record.value(), PaymentRefundedEvent.class);
        assertThat(event.orderId()).isEqualTo(orderId);
        assertThat(event.username()).isEqualTo("john_doe");

        List<PaymentEntity> payments = paymentRepository.findAll();
        assertThat(payments).hasSize(2);
        boolean hasRefundRecord = payments.stream()
                .anyMatch(p -> p.getType() == PaymentType.REFUND && "re_stripe_ok_123".equals(p.getStripeId()));
        assertThat(hasRefundRecord).isTrue();
    }

    @Test
    @Order(3)
    void should_PublishToErrorsCompensatingEventsWithRetryableException_When_RefundPaymentFailsWithRetryableStatus() throws Exception {
        // Arrange
        UUID orderId = UUID.randomUUID();
        paymentRepository.save(PaymentEntity.builder()
                .userId(UUID.randomUUID())
                .orderId(orderId)
                .stripeId("ch_stripe_123")
                .amount(new BigDecimal("100.00"))
                .type(PaymentType.PAYMENT)
                .build());

        RefundPaymentCommand command = new RefundPaymentCommand(
                orderId,
                "john_doe"
        );
        String messageId = UUID.randomUUID().toString();
        when(stripeService.createRefundPayment(any()))
                .thenThrow(new PaymentFailedException("Stripe transient limit", 403));

        // Act
        sendMessage(env.getRequiredProperty("payment.commands.topic.name"),
                orderId.toString(), command, messageId);

        // Assert
        ConsumerRecord<String, String> record =
                KafkaTestUtils.getSingleRecord(spyConsumer, "errors-compensating-events", Duration.ofSeconds(12));
        assertThat(record.key()).isEqualTo(orderId.toString());

        PaymentRefundFailedEvent failedEvent = mapper.readValue(record.value(), PaymentRefundFailedEvent.class);
        assertThat(failedEvent.orderId()).isEqualTo(orderId);
        assertThat(failedEvent.username()).isEqualTo("john_doe");
    }

    @Test
    @Order(1)
    void should_PublishToErrorsCompensatingEventsWithNotRetryableException_When_RefundPaymentFailsWithNotRetryableStatus() throws Exception {
        // Arrange
        UUID orderId = UUID.randomUUID();
        paymentRepository.save(PaymentEntity.builder()
                .userId(UUID.randomUUID())
                .orderId(orderId)
                .stripeId("ch_stripe_123")
                .amount(new BigDecimal("100.00"))
                .type(PaymentType.PAYMENT)
                .build());

        RefundPaymentCommand command = new RefundPaymentCommand(
                orderId,
                "john_doe"
        );
        String messageId = UUID.randomUUID().toString();
        when(stripeService.createRefundPayment(any()))
                .thenThrow(new PaymentFailedException("Stripe fatal block", 0));

        // Act
        sendMessage(env.getRequiredProperty("payment.commands.topic.name"),
                orderId.toString(), command, messageId);

        // Assert
        ConsumerRecord<String, String> record =
                KafkaTestUtils.getSingleRecord(spyConsumer, "errors-compensating-events", Duration.ofSeconds(5));
        assertThat(record.key()).isEqualTo(orderId.toString());

        PaymentRefundFailedEvent failedEvent = mapper.readValue(record.value(), PaymentRefundFailedEvent.class);
        assertThat(failedEvent.orderId()).isEqualTo(orderId);
    }

    @Test
    @Order(2)
    void should_SkipProcessing_When_DuplicateChargePaymentCommandReceived() {
        // Arrange
        UUID orderId = UUID.randomUUID();
        ChargePaymentCommand command = new ChargePaymentCommand(
                orderId, UUID.randomUUID(),
                "user",
                new BigDecimal("10.00")
        );
        String messageId = "msg-id-already-processed-777";

        eventRepository.save(EventEntity.builder().messageId(messageId).build());

        // Act
        sendMessage(env.getRequiredProperty("payment.commands.topic.name"),
                orderId.toString(), command, messageId);

        // Assert
        assertThrows(Exception.class,
                () -> KafkaTestUtils.getSingleRecord(spyConsumer, "successful-events", Duration.ofSeconds(3)));
    }
}
