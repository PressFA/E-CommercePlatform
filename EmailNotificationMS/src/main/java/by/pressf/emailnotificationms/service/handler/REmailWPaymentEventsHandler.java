package by.pressf.emailnotificationms.service.handler;

import by.pressf.core.dto.choreography.events.BalanceTopUpCompletedEvent;
import by.pressf.core.exceptions.NotRetryableException;
import by.pressf.core.exceptions.RetryableException;
import by.pressf.emailnotificationms.dao.entity.EventEntity;
import by.pressf.emailnotificationms.dao.repository.EventRepository;
import by.pressf.emailnotificationms.service.EmailService;
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
@KafkaListener(topics = "${r-email-w-payment.topic.name}", groupId = "email-ms")
public class REmailWPaymentEventsHandler {
    private final EmailService emailService;
    private final EventRepository eventRepository;

    @KafkaHandler
    @Transactional("transactionManager")
    public void handle(@Payload BalanceTopUpCompletedEvent event,
                       @Header("messageId") String messageId) {
        try {
            log.info("The BalanceTopUpCompletedEvent event from the r-email-w-payment-events topic has been received");

            EventEntity processedEvent = eventRepository.findByMessageId(messageId);
            if (processedEvent != null) {
                log.info("The BalanceTopUpCompletedEvent message with messageId={} has already been processed", messageId);
                return;
            }

            emailService.sendEmail(event.email(), event.subject(), event.body());
            log.info("The email was successfully delivered to the post office {}", event.email());

            eventRepository.save(EventEntity.builder()
                    .messageId(messageId)
                    .build());
            log.info("The BalanceTopUpCompletedEvent message with messageId={} has been processed", messageId);
        } catch (MailSendException e) {
            log.error(e.getMessage());
            throw new RetryableException(e);
        } catch (MailException | DataAccessException e) {
            log.error(e.getMessage());
            throw new NotRetryableException(e);
        }
    }
}
