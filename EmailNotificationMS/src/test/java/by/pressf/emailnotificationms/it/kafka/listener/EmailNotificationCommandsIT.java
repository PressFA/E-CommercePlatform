package by.pressf.emailnotificationms.it.kafka.listener;

import by.pressf.core.dto.orchestration.commands.emailnotification.SendEmailOrderCommand;
import by.pressf.emailnotificationms.dao.entity.EventEntity;
import by.pressf.emailnotificationms.dao.repository.EventRepository;
import by.pressf.emailnotificationms.it.config.BaseIT;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.mail.MailAuthenticationException;
import org.springframework.mail.MailSendException;
import org.springframework.mail.SimpleMailMessage;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class EmailNotificationCommandsIT extends BaseIT {
    @Autowired
    private EventRepository eventRepository;

    @BeforeEach
    void setUp() { spyConsumer.poll(Duration.ofMillis(100)); }

    @Test @Order(1)
    void should_ProcessCommandSuccessfullyAndNotPublishToDlt_When_CommandIsValid() {
        // Arrange
        SendEmailOrderCommand command = new SendEmailOrderCommand(
                "test@mail.com",
                "subject",
                "body"
        );

        // Act
        sendMessage(env.getRequiredProperty("email-notification.commands.topic.name"),
                UUID.randomUUID().toString(), command, UUID.randomUUID().toString());

        // Assert
        assertThrows(Exception.class, () ->
                KafkaTestUtils.getSingleRecord(spyConsumer, TOPIC, Duration.ofSeconds(3)));
    }

    @Test @Order(1)
    void should_IgnoreCommandAndNotPublishToDlt_When_MessageIsDuplicate() {
        // Arrange
        String messageId = UUID.randomUUID().toString();
        eventRepository.save(EventEntity.builder().messageId(messageId).build());

        SendEmailOrderCommand command = new SendEmailOrderCommand(
                "test@mail.com",
                "subject",
                "body"
        );

        // Act
        sendMessage(env.getRequiredProperty("email-notification.commands.topic.name"),
                UUID.randomUUID().toString(), command, messageId);

        // Assert
        assertThrows(Exception.class, () ->
                KafkaTestUtils.getSingleRecord(spyConsumer, TOPIC, Duration.ofSeconds(3)));
    }

    @Test @Order(2)
    void should_PublishToDltAfterRetries_When_MailSenderThrowsMailSendException() {
        // Arrange
        String messageKey = UUID.randomUUID().toString();

        SendEmailOrderCommand command = new SendEmailOrderCommand(
                "test@mail.com",
                "subject",
                "body"
        );

        doThrow(new MailSendException("SMTP server connection timeout"))
                .when(mailSender).send(any(SimpleMailMessage.class));

        // Act
        sendMessage(env.getRequiredProperty("email-notification.commands.topic.name"),
                messageKey, command, UUID.randomUUID().toString());

        ConsumerRecord<String, String> record =
                KafkaTestUtils.getSingleRecord(spyConsumer, TOPIC, Duration.ofSeconds(12));

        // Assert
        assertThat(record.key()).isEqualTo(messageKey);
        assertThat(record.value()).isNotNull();
        assertThat(record.headers().lastHeader("messageId")).isNotNull();

        SendEmailOrderCommand command2 = mapper.readValue(record.value(), SendEmailOrderCommand.class);
        assertThat(command2).isEqualTo(command);
    }

    @Test @Order(1)
    void should_PublishToDltImmediatelyWithoutRetries_When_MailSenderThrowsMailAuthenticationException() {
        // Arrange
        String messageKey = UUID.randomUUID().toString();

        SendEmailOrderCommand command = new SendEmailOrderCommand(
                "test@mail.com",
                "subject",
                "body"
        );

        doThrow(new MailAuthenticationException("Invalid SMTP credentials"))
                .when(mailSender).send(any(SimpleMailMessage.class));

        // Act
        sendMessage(env.getRequiredProperty("email-notification.commands.topic.name"),
                messageKey, command, UUID.randomUUID().toString());

        ConsumerRecord<String, String> record =
                KafkaTestUtils.getSingleRecord(spyConsumer, TOPIC, Duration.ofSeconds(5));

        // Assert
        assertThat(record.key()).isEqualTo(messageKey);
        assertThat(record.value()).isNotNull();
        assertThat(record.headers().lastHeader("messageId")).isNotNull();

        SendEmailOrderCommand command2 = mapper.readValue(record.value(), SendEmailOrderCommand.class);
        assertThat(command2).isEqualTo(command);
    }
}
