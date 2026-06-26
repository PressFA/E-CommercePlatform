package by.pressf.emailnotificationms.it.kafka.listener;

import by.pressf.core.dto.choreography.events.BalanceTopUpFailedEvent;
import by.pressf.emailnotificationms.it.config.BaseIT;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.*;
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
public class REmailWUserEventsIT extends BaseIT {
    @BeforeEach
    void setUp() { spyConsumer.poll(Duration.ofMillis(100)); }

    @Test @Order(1)
    void should_ProcessEventSuccessfullyAndNotPublishToDlt_When_EventIsValid() {
        // Arrange
        BalanceTopUpFailedEvent event = new BalanceTopUpFailedEvent(
                "user@mail.com",
                "Failed Subject",
                "Your balance top-up failed"
        );

        // Act
        sendMessage(env.getRequiredProperty("r-email-w-user.topic.name"),
                UUID.randomUUID().toString(), event, UUID.randomUUID().toString());

        // Assert
        assertThrows(Exception.class, () ->
                KafkaTestUtils.getSingleRecord(spyConsumer, TOPIC, Duration.ofSeconds(3)));
    }

    @Test @Order(2)
    void should_PublishToDltAfterRetries_When_MailSenderThrowsMailSendException() {
        // Arrange
        String messageKey = UUID.randomUUID().toString();
        BalanceTopUpFailedEvent event = new BalanceTopUpFailedEvent(
                "user@mail.com",
                "Failed Subject",
                "Your balance top-up failed"
        );

        doThrow(new MailSendException("SMTP connection timeout"))
                .when(mailSender).send(any(SimpleMailMessage.class));

        // Act
        sendMessage(env.getRequiredProperty("r-email-w-user.topic.name"),
                messageKey, event, UUID.randomUUID().toString());

        ConsumerRecord<String, String> record =
                KafkaTestUtils.getSingleRecord(spyConsumer, TOPIC, Duration.ofSeconds(12));

        // Assert
        assertThat(record.key()).isEqualTo(messageKey);
        assertThat(record.value()).isNotNull();
        assertThat(record.headers().lastHeader("messageId")).isNotNull();

        BalanceTopUpFailedEvent receivedEvent = mapper.readValue(record.value(), BalanceTopUpFailedEvent.class);
        assertThat(receivedEvent).isEqualTo(event);
    }

    @Test @Order(1)
    void should_PublishToDltImmediatelyWithoutRetries_When_MailSenderThrowsMailAuthenticationException() {
        // Arrange
        String messageKey = UUID.randomUUID().toString();
        BalanceTopUpFailedEvent event = new BalanceTopUpFailedEvent(
                "user@mail.com",
                "Failed Subject",
                "Your balance top-up failed"
        );

        doThrow(new MailAuthenticationException("Bad credentials"))
                .when(mailSender).send(any(SimpleMailMessage.class));

        // Act
        sendMessage(env.getRequiredProperty("r-email-w-user.topic.name"),
                messageKey, event, UUID.randomUUID().toString());

        ConsumerRecord<String, String> record =
                KafkaTestUtils.getSingleRecord(spyConsumer, TOPIC, Duration.ofSeconds(5));

        // Assert
        assertThat(record.key()).isEqualTo(messageKey);
        assertThat(record.value()).isNotNull();
        assertThat(record.headers().lastHeader("messageId")).isNotNull();

        BalanceTopUpFailedEvent receivedEvent = mapper.readValue(record.value(), BalanceTopUpFailedEvent.class);
        assertThat(receivedEvent).isEqualTo(event);
    }
}
