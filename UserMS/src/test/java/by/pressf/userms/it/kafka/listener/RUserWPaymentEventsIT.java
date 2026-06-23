package by.pressf.userms.it.kafka.listener;

import by.pressf.core.dto.choreography.events.BalanceTopUpFailedEvent;
import by.pressf.core.dto.choreography.events.UserBalanceCreditFailedEvent;
import by.pressf.userms.dao.entity.UserEntity;
import by.pressf.userms.it.config.BaseIT;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.*;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doThrow;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class RUserWPaymentEventsIT extends BaseIT {
    @BeforeAll
    static void init() { spyConsumer = createSpyConsumer(List.of("r-email-w-user-events", "user-ms-dlt")); }

    @BeforeEach
    void setUp() {
        spyConsumer.poll(Duration.ofMillis(100));
        userRepository.deleteAll();
    }

    @AfterAll
    static void destruct() { spyConsumer.close(); }

    @Test @Order(1)
    void should_AdjustBalanceAndPublishEmailEvent_When_CreditFailedEventIsValid() {
        // Arrange
        UserEntity user = userRepository.save(UserEntity.builder()
                .username("test@mail.com")
                .password("12345")
                .name("Danny")
                .balance(new BigDecimal("110.00"))
                .build());

        String topic = env.getRequiredProperty("r-email-w-user.topic.name");

        UserBalanceCreditFailedEvent event = new UserBalanceCreditFailedEvent(
                user.getId(),
                user.getUsername(),
                BigDecimal.TEN
        );

        // Act
        sendMessage(env.getRequiredProperty("r-user-w-payment.topic.name"), event.userId().toString(),
                event, UUID.randomUUID().toString());

        ConsumerRecord<String, String> record =
                KafkaTestUtils.getSingleRecord(spyConsumer, topic, Duration.ofSeconds(5));

        // Assert
        assertThat(record.key()).isEqualTo(event.userId().toString());
        assertThat(record.value()).isNotNull();
        assertThat(record.headers().lastHeader("messageId")).isNotNull();

        BalanceTopUpFailedEvent failedEvent = mapper.readValue(record.value(), BalanceTopUpFailedEvent.class);
        assertThat(failedEvent.email()).isEqualTo(event.email());
        assertThat(failedEvent.subject().length()).isEqualTo(21);
        assertThat(failedEvent.body().length()).isGreaterThan(123);

        user = userRepository.findById(user.getId())
                .orElseThrow(() -> new AssertionError("Пользователь не найден в БД"));
        assertThat(user.getBalance()).isEqualByComparingTo(new BigDecimal("100.00"));
    }

    @Test @Order(2)
    void should_PublishToDltAfterRetries_When_OptimisticLockingFailureOccurs() {
        // Arrange
        UserEntity user = userRepository.save(UserEntity.builder()
                .username("test@mail.com")
                .password("12345")
                .name("Danny")
                .balance(BigDecimal.TEN)
                .build());

        doThrow(new OptimisticLockingFailureException("!!!errors!!!")).when(userRepository)
                .save(argThat(u -> "test@mail.com".equals(u.getUsername())));

        String topic = env.getRequiredProperty("user.dlt.name");

        UserBalanceCreditFailedEvent event = new UserBalanceCreditFailedEvent(
                user.getId(),
                user.getUsername(),
                BigDecimal.TEN
        );

        // Act
        sendMessage(env.getRequiredProperty("r-user-w-payment.topic.name"), event.userId().toString(),
                event, UUID.randomUUID().toString());

        ConsumerRecord<String, String> record =
                KafkaTestUtils.getSingleRecord(spyConsumer, topic, Duration.ofSeconds(12));

        // Assert
        assertThat(record.key()).isEqualTo(event.userId().toString());
        assertThat(record.value()).isNotNull();
        assertThat(record.headers().lastHeader("messageId")).isNotNull();

        UserBalanceCreditFailedEvent failedEvent
                = mapper.readValue(record.value(), UserBalanceCreditFailedEvent.class);
        assertThat(failedEvent.userId()).isEqualTo(event.userId());
        assertThat(failedEvent.email()).isEqualTo(event.email());
        assertThat(failedEvent.amount()).isEqualTo(event.amount());

        UserEntity user2 = userRepository.findById(user.getId())
                .orElseThrow(() -> new AssertionError("Пользователь не найден в БД"));
        assertThat(user2).isEqualTo(user);
    }

    @Test @Order(1)
    void should_PublishToDltImmediatelyWithoutRetries_When_UserNotFound() {
        // Arrange
        String topic = env.getRequiredProperty("user.dlt.name");

        UserBalanceCreditFailedEvent event = new UserBalanceCreditFailedEvent(
                UUID.randomUUID(),
                "test@mail.com",
                BigDecimal.TEN
        );

        // Act
        sendMessage(env.getRequiredProperty("r-user-w-payment.topic.name"), event.userId().toString(),
                event, UUID.randomUUID().toString());

        ConsumerRecord<String, String> record =
                KafkaTestUtils.getSingleRecord(spyConsumer, topic, Duration.ofSeconds(5));

        // Assert
        assertThat(record.key()).isEqualTo(event.userId().toString());
        assertThat(record.value()).isNotNull();
        assertThat(record.headers().lastHeader("messageId")).isNotNull();

        UserBalanceCreditFailedEvent failedEvent
                = mapper.readValue(record.value(), UserBalanceCreditFailedEvent.class);
        assertThat(failedEvent.userId()).isEqualTo(event.userId());
        assertThat(failedEvent.email()).isEqualTo(event.email());
        assertThat(failedEvent.amount()).isEqualTo(event.amount());
    }
}
