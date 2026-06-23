package by.pressf.paymentms.it.kafka.listener;

import by.pressf.core.dto.choreography.events.BalanceTopUpCompletedEvent;
import by.pressf.core.dto.choreography.events.UserBalanceCreditFailedEvent;
import by.pressf.core.dto.choreography.events.UserBalanceCreditedEvent;
import by.pressf.paymentms.dao.entity.PaymentEntity;
import by.pressf.paymentms.dao.entity.type.PaymentType;
import by.pressf.paymentms.exception.PaymentFailedException;
import by.pressf.paymentms.it.config.BaseIT;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.*;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class RPaymentWUserEventsIT extends BaseIT {
    @BeforeAll
    static void init() {
        spyConsumer = createSpyConsumer(List.of("r-email-w-payment-events", "r-user-w-payment-events"));
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
    void should_ProcessUserBalanceCreditedSuccessfully_When_EventReceived() throws Exception {
        // Arrange
        UUID userId = UUID.randomUUID();
        String email = "dev@pressf.by";
        BigDecimal amount = new BigDecimal("300.00");
        String messageId = UUID.randomUUID().toString();

        UserBalanceCreditedEvent event = new UserBalanceCreditedEvent(
                userId,
                email,
                amount
        );
        when(stripeService.createTopUpPayment(any())).thenReturn("ch_topup_ok_555");

        // Act
        sendMessage(env.getRequiredProperty("r-payment-w-user.topic.name"), userId.toString(), event, messageId);

        // Assert
        ConsumerRecord<String, String> record =
                KafkaTestUtils.getSingleRecord(spyConsumer, "r-email-w-payment-events", Duration.ofSeconds(5));
        assertThat(record.key()).isEqualTo(userId.toString());

        BalanceTopUpCompletedEvent completedEvent = mapper.readValue(record.value(), BalanceTopUpCompletedEvent.class);
        assertThat(completedEvent.email()).isEqualTo(email);
        assertThat(completedEvent.subject()).isEqualTo("Balance Topped Up");
        assertThat(completedEvent.body()).contains(amount.toString());

        List<PaymentEntity> payments = paymentRepository.findAll();
        assertThat(payments).hasSize(1);
        PaymentEntity savedPayment = payments.getFirst();
        assertThat(savedPayment.getUserId()).isEqualTo(userId);
        assertThat(savedPayment.getType()).isEqualTo(PaymentType.TOP_UP);
    }

    @Test
    @Order(2)
    void should_ThrowRetryableExceptionAndNotPublishSuccess_When_PaymentFailsWithRetryableStatus() throws Exception {
        // Arrange
        UUID userId = UUID.randomUUID();
        String email = "test@test.com";
        BigDecimal amount = new BigDecimal("50.00");
        String messageId = UUID.randomUUID().toString();

        UserBalanceCreditedEvent event = new UserBalanceCreditedEvent(userId, email, amount);

        when(stripeService.createTopUpPayment(any()))
                .thenThrow(new PaymentFailedException("Stripe network timeout", 404));

        // Act
        sendMessage(env.getRequiredProperty("r-payment-w-user.topic.name"), userId.toString(), event, messageId);

        // Assert
        ConsumerRecord<String, String> record =
                KafkaTestUtils.getSingleRecord(spyConsumer, "r-user-w-payment-events", Duration.ofSeconds(12));
        assertThat(record.key()).isEqualTo(userId.toString());

        UserBalanceCreditFailedEvent failedEvent =
                mapper.readValue(record.value(), UserBalanceCreditFailedEvent.class);
        assertThat(failedEvent.userId()).isEqualTo(userId);
        assertThat(failedEvent.email()).isEqualTo(email);
        assertThat(failedEvent.amount()).isEqualByComparingTo(amount);

        assertThat(paymentRepository.findAll()).isEmpty();
    }

    @Test
    @Order(1)
    void should_ThrowNotRetryableExceptionAndNotPublishSuccess_When_PaymentFailsWithNotRetryableStatus() throws Exception {
        // Arrange
        UUID userId = UUID.randomUUID();
        String email = "test@test.com";
        BigDecimal amount = new BigDecimal("50.00");
        String messageId = UUID.randomUUID().toString();

        UserBalanceCreditedEvent event = new UserBalanceCreditedEvent(userId, email, amount);

        when(stripeService.createTopUpPayment(any()))
                .thenThrow(new PaymentFailedException("Internal fraud error", 500));

        // Act
        sendMessage(env.getRequiredProperty("r-payment-w-user.topic.name"), userId.toString(), event, messageId);

        // Assert
        ConsumerRecord<String, String> record =
                KafkaTestUtils.getSingleRecord(spyConsumer, "r-user-w-payment-events", Duration.ofSeconds(5));
        assertThat(record.key()).isEqualTo(userId.toString());

        UserBalanceCreditFailedEvent failedEvent =
                mapper.readValue(record.value(), UserBalanceCreditFailedEvent.class);
        assertThat(failedEvent.userId()).isEqualTo(userId);
        assertThat(failedEvent.email()).isEqualTo(email);
        assertThat(failedEvent.amount()).isEqualByComparingTo(amount);

        assertThat(paymentRepository.findAll()).isEmpty();
    }
}
