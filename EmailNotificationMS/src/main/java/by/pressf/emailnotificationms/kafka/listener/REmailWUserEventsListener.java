package by.pressf.emailnotificationms.kafka.listener;

import by.pressf.core.dto.choreography.events.BalanceTopUpFailedEvent;
import by.pressf.core.exceptions.DuplicateMessageException;
import by.pressf.core.exceptions.NotRetryableException;
import by.pressf.core.exceptions.RetryableException;
import by.pressf.emailnotificationms.service.EmailService;
import by.pressf.emailnotificationms.service.IdempotencyService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.mail.MailException;
import org.springframework.mail.MailSendException;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Component
@RequiredArgsConstructor
@KafkaListener(topics = "${r-email-w-user.topic.name}", groupId = "email-ms")
public class REmailWUserEventsListener {
    private final EmailService emailService;
    private final IdempotencyService idempotencyService;

    @KafkaHandler
    @Transactional
    public void handle(@Payload BalanceTopUpFailedEvent event,
                       @Header("messageId") String messageId) {
        try {
            log.warn("The BalanceTopUpFailedEvent event from the r-email-w-user-events topic has been received");

            idempotencyService.idempotenceCheck(messageId, event.getClass().getSimpleName());

            emailService.sendEmail(event.email(), event.subject(), event.body());
            log.warn("The email was successfully delivered to the post office {}", event.email());

            idempotencyService.saveIdempotentKey(messageId, event.getClass().getSimpleName());
        } catch (DuplicateMessageException e) {
            log.warn(e.getMessage());
        } catch (MailSendException e) {
            log.error(e.getMessage());
            throw new RetryableException(e);
        } catch (MailException | DataAccessException e) {
            log.error(e.getMessage());
            throw new NotRetryableException(e);
        }
    }
}
