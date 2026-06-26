package by.pressf.emailnotificationms.kafka.listener;

import by.pressf.core.dto.orchestration.commands.emailnotification.SendEmailOrderCommand;
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
@KafkaListener(topics = "${email-notification.commands.topic.name}", groupId = "email-ms")
public class EmailCommandsListener {
    private final EmailService emailService;
    private final IdempotencyService idempotencyService;

    @KafkaHandler
    @Transactional
    public void handleCommand(@Payload SendEmailOrderCommand command,
                              @Header("messageId") String messageId) {
        try {
            log.info("The SendEmailOrderCommand command from the email-notification-commands topic has been received");

            idempotencyService.idempotenceCheck(messageId, command.getClass().getSimpleName());

            emailService.sendEmail(command.email(), command.subject(), command.body());
            log.info("The email was successfully delivered to the post office {}", command.email());

            idempotencyService.saveIdempotentKey(messageId, command.getClass().getSimpleName());
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
