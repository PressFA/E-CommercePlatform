package by.pressf.userms.it.kafka.listener;

import by.pressf.core.dto.orchestration.commands.user.CancelUserBalanceDebitCommand;
import by.pressf.core.dto.orchestration.commands.user.DebitUserBalanceCommand;
import by.pressf.core.dto.orchestration.events.user.UserBalanceDebitCancelFailedEvent;
import by.pressf.core.dto.orchestration.events.user.UserBalanceDebitCanceledEvent;
import by.pressf.core.dto.orchestration.events.user.UserBalanceDebitFailedEvent;
import by.pressf.core.dto.orchestration.events.user.UserBalanceDebitedEvent;
import by.pressf.userms.dao.entity.EventEntity;
import by.pressf.userms.dao.entity.UserEntity;
import by.pressf.userms.dao.repository.EventRepository;
import by.pressf.userms.it.config.BaseIT;
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
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doThrow;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class UserCommandsTopicIT extends BaseIT {
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
        userRepository.deleteAll();
    }

    @AfterAll
    static void destruct() { spyConsumer.close(); }

    @Test @Order(1)
    void should_DebitBalanceAndPublishSuccessfulEvent_When_DebitCommandIsValid() {
        // Arrange
        UserEntity user = userRepository.save(UserEntity.builder()
                .username("test@mail.com")
                .password("12345")
                .name("Danny")
                .balance(new BigDecimal("100.00"))
                .build());

        String topic = env.getRequiredProperty("successful-events.topic.name");

        DebitUserBalanceCommand command = new DebitUserBalanceCommand(
                UUID.randomUUID(),
                user.getId(),
                user.getUsername(),
                BigDecimal.TEN
        );

        // Act
        sendMessage(env.getRequiredProperty("user.commands.topic.name"), command.orderId().toString(),
                command, UUID.randomUUID().toString());

        ConsumerRecord<String, String> record =
                KafkaTestUtils.getSingleRecord(spyConsumer, topic, Duration.ofSeconds(5));

        // Assert
        assertThat(record.key()).isEqualTo(command.orderId().toString());
        assertThat(record.value()).isNotNull();
        assertThat(record.headers().lastHeader("messageId")).isNotNull();

        UserBalanceDebitedEvent event = mapper.readValue(record.value(), UserBalanceDebitedEvent.class);
        assertThat(event.orderId()).isEqualTo(command.orderId());
        assertThat(event.userId()).isEqualTo(command.userId());
        assertThat(event.username()).isEqualTo(command.username());
        assertThat(event.amount()).isEqualByComparingTo(command.amount());

        user = userRepository.findById(user.getId())
                .orElseThrow(() -> new AssertionError("Пользователь не найден в БД"));
        assertThat(user.getBalance()).isEqualByComparingTo(new BigDecimal("90.00"));
    }

    @Test @Order(1)
    void should_NotPublishToSuccessfulTopic_When_UserNotFoundDuringDebit() {
        // Arrange
        String messageId = UUID.randomUUID().toString();
        eventRepository.save(EventEntity.builder().messageId(messageId).build());

        UserEntity user = userRepository.save(UserEntity.builder()
                .username("test@mail.com")
                .password("12345")
                .name("Danny")
                .balance(new BigDecimal("100.00"))
                .build());

        String topic = env.getRequiredProperty("successful-events.topic.name");

        DebitUserBalanceCommand command = new DebitUserBalanceCommand(
                UUID.randomUUID(),
                user.getId(),
                user.getUsername(),
                BigDecimal.TEN
        );

        // Act
        sendMessage(env.getRequiredProperty("user.commands.topic.name"), command.orderId().toString(),
                command, messageId);

        // Assert
        assertThrows(Exception.class,
                () -> KafkaTestUtils.getSingleRecord(spyConsumer, topic, Duration.ofSeconds(3)));

        user = userRepository.findById(user.getId())
                .orElseThrow(() -> new AssertionError("Пользователь не найден в БД"));
        assertThat(user.getBalance()).isEqualByComparingTo(new BigDecimal("100.00"));
    }

    @Test @Order(2)
    void should_PublishToErrorsSuccessfulTopic_When_OptimisticLockingFailureDuringDebit() {
        // Arrange
        UserEntity user = userRepository.save(UserEntity.builder()
                .username("test@mail.com")
                .password("12345")
                .name("Danny")
                .balance(new BigDecimal("100.00"))
                .build());

        doThrow(new OptimisticLockingFailureException("!!!errors!!!")).when(userRepository)
                .save(argThat(u -> "test@mail.com".equals(u.getUsername())));

        String topic = env.getRequiredProperty("errors-successful-events.topic.name");

        DebitUserBalanceCommand command = new DebitUserBalanceCommand(
                UUID.randomUUID(),
                user.getId(),
                user.getUsername(),
                BigDecimal.TEN
        );

        // Act
        sendMessage(env.getRequiredProperty("user.commands.topic.name"), command.orderId().toString(),
                command, UUID.randomUUID().toString());

        ConsumerRecord<String, String> record =
                KafkaTestUtils.getSingleRecord(spyConsumer, topic, Duration.ofSeconds(12));

        // Assert
        assertThat(record.key()).isEqualTo(command.orderId().toString());
        assertThat(record.value()).isNotNull();
        assertThat(record.headers().lastHeader("messageId")).isNotNull();

        UserBalanceDebitFailedEvent event = mapper.readValue(record.value(), UserBalanceDebitFailedEvent.class);
        assertThat(event.orderId()).isEqualTo(command.orderId());
        assertThat(event.username()).isEqualTo(command.username());

        UserEntity user2 = userRepository.findById(user.getId())
                .orElseThrow(() -> new AssertionError("Пользователь не найден в БД"));
        assertThat(user2).isEqualTo(user);
    }

    @Test @Order(1)
    void should_PublishToErrorsSuccessfulTopic_When_InsufficientBalanceDuringDebit() {
        // Arrange
        UserEntity user = userRepository.save(UserEntity.builder()
                .username("test@mail.com")
                .password("12345")
                .name("Danny")
                .balance(BigDecimal.ONE)
                .build());

        String topic = env.getRequiredProperty("errors-successful-events.topic.name");

        DebitUserBalanceCommand command = new DebitUserBalanceCommand(
                UUID.randomUUID(),
                user.getId(),
                user.getUsername(),
                BigDecimal.TWO
        );

        // Act
        sendMessage(env.getRequiredProperty("user.commands.topic.name"), command.orderId().toString(),
                command, UUID.randomUUID().toString());

        ConsumerRecord<String, String> record =
                KafkaTestUtils.getSingleRecord(spyConsumer, topic, Duration.ofSeconds(5));

        // Assert
        assertThat(record.key()).isEqualTo(command.orderId().toString());
        assertThat(record.value()).isNotNull();
        assertThat(record.headers().lastHeader("messageId")).isNotNull();

        UserBalanceDebitFailedEvent event = mapper.readValue(record.value(), UserBalanceDebitFailedEvent.class);
        assertThat(event.orderId()).isEqualTo(command.orderId());
        assertThat(event.username()).isEqualTo(command.username());

        user = userRepository.findById(user.getId())
                .orElseThrow(() -> new AssertionError("Пользователь не найден в БД"));
        assertThat(user.getBalance()).isEqualByComparingTo(BigDecimal.ONE);
    }

    @Test @Order(2)
    void should_PublishToErrorsCompensatingTopic_When_OptimisticLockingFailureDuringCancel() {
        // Arrange
        UserEntity user = userRepository.save(UserEntity.builder()
                .username("test@mail.com")
                .password("12345")
                .name("Danny")
                .balance(new BigDecimal("100.00"))
                .build());

        doThrow(new OptimisticLockingFailureException("!!!errors!!!")).when(userRepository)
                .save(argThat(u -> "test@mail.com".equals(u.getUsername())));

        String topic = env.getRequiredProperty("errors-compensating-events.topic.name");

        CancelUserBalanceDebitCommand command = new CancelUserBalanceDebitCommand(
                UUID.randomUUID(),
                user.getId(),
                user.getUsername(),
                BigDecimal.TEN
        );

        // Act
        sendMessage(env.getRequiredProperty("user.commands.topic.name"), command.orderId().toString(),
                command, UUID.randomUUID().toString());

        ConsumerRecord<String, String> record =
                KafkaTestUtils.getSingleRecord(spyConsumer, topic, Duration.ofSeconds(12));

        // Assert
        assertThat(record.key()).isEqualTo(command.orderId().toString());
        assertThat(record.value()).isNotNull();
        assertThat(record.headers().lastHeader("messageId")).isNotNull();

        UserBalanceDebitCancelFailedEvent event =
                mapper.readValue(record.value(), UserBalanceDebitCancelFailedEvent.class);
        assertThat(event.orderId()).isEqualTo(command.orderId());
        assertThat(event.username()).isEqualTo(command.username());

        UserEntity user2 = userRepository.findById(user.getId())
                .orElseThrow(() -> new AssertionError("Пользователь не найден в БД"));
        assertThat(user2).isEqualTo(user);
    }

    @Test @Order(1)
    void should_PublishToErrorsCompensatingTopic_When_UserNotFoundDuringCancel() {
        // Arrange
        String topic = env.getRequiredProperty("errors-compensating-events.topic.name");

        CancelUserBalanceDebitCommand command = new CancelUserBalanceDebitCommand(
                UUID.randomUUID(),
                UUID.randomUUID(),
                "test@mail.com",
                BigDecimal.TEN
        );

        // Act
        sendMessage(env.getRequiredProperty("user.commands.topic.name"), command.orderId().toString(),
                command, UUID.randomUUID().toString());

        ConsumerRecord<String, String> record =
                KafkaTestUtils.getSingleRecord(spyConsumer, topic, Duration.ofSeconds(5));

        // Assert
        assertThat(record.key()).isEqualTo(command.orderId().toString());
        assertThat(record.value()).isNotNull();
        assertThat(record.headers().lastHeader("messageId")).isNotNull();

        UserBalanceDebitCancelFailedEvent event =
                mapper.readValue(record.value(), UserBalanceDebitCancelFailedEvent.class);
        assertThat(event.orderId()).isEqualTo(command.orderId());
        assertThat(event.username()).isEqualTo(command.username());
    }

    @Test @Order(1)
    void should_RestoreBalanceAndPublishCompensatingEvent_When_CancelCommandIsValid() {
        // Arrange
        UserEntity user = userRepository.save(UserEntity.builder()
                .username("test@mail.com")
                .password("12345")
                .name("Danny")
                .balance(new BigDecimal("90.00"))
                .build());

        String topic = env.getRequiredProperty("compensating-events.topic.name");

        CancelUserBalanceDebitCommand command = new CancelUserBalanceDebitCommand(
                UUID.randomUUID(),
                user.getId(),
                user.getUsername(),
                BigDecimal.TEN
        );

        // Act
        sendMessage(env.getRequiredProperty("user.commands.topic.name"), command.orderId().toString(),
                command, UUID.randomUUID().toString());

        ConsumerRecord<String, String> record =
                KafkaTestUtils.getSingleRecord(spyConsumer, topic, Duration.ofSeconds(5));

        // Assert
        assertThat(record.key()).isEqualTo(command.orderId().toString());
        assertThat(record.value()).isNotNull();
        assertThat(record.headers().lastHeader("messageId")).isNotNull();

        UserBalanceDebitCanceledEvent event =
                mapper.readValue(record.value(), UserBalanceDebitCanceledEvent.class);
        assertThat(event.orderId()).isEqualTo(command.orderId());
        assertThat(event.username()).isEqualTo(command.username());

        user = userRepository.findById(user.getId())
                .orElseThrow(() -> new AssertionError("Пользователь не найден в БД"));
        assertThat(user.getBalance()).isEqualByComparingTo(new BigDecimal("100.00"));
    }
}
