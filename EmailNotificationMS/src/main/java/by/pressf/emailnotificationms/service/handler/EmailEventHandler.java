package by.pressf.emailnotificationms.service.handler;

import by.pressf.core.dto.events.emailnotification.EmailMessage;
import by.pressf.core.exceptions.NotRetryableException;
import by.pressf.emailnotificationms.dao.entity.EventEntity;
import by.pressf.emailnotificationms.dao.repository.EventRepository;
import by.pressf.emailnotificationms.service.EmailService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
@KafkaListener(topics = "${email-notification.events.topic.name}", groupId = "email-ms")
public class EmailEventHandler {
    private final EmailService emailService;
    private final EventRepository eventRepository;

    @KafkaHandler
    public void handle(@Payload EmailMessage event,
                       @Header("messageId") String messageId) {
        try {
            log.info("The EmailMessage event from the send-notification-event topic has been received");

            EventEntity processedEvent = eventRepository.findByMessageId(messageId);
            if (processedEvent != null) {
                log.info("The EmailMessage message with messageId={} has already been processed", messageId);
                return;
            }

            emailService.sendEmail(event.email(), event.subject(), event.body());
            log.info("The email was successfully delivered to the post office {}", event.email());

            eventRepository.save(EventEntity.builder()
                    .messageId(messageId)
                    .build());
            log.info("The EmailMessage message with messageId={} has been processed", messageId);
        } catch (DataAccessException e) {
            log.error(e.getMessage());
            throw new NotRetryableException(e);
        }
    }
}
