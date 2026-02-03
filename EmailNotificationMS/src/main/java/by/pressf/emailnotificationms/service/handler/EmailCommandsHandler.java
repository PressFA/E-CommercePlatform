package by.pressf.emailnotificationms.service.handler;

import by.pressf.core.dto.orchestration.commands.emailnotification.SendEmailOrderCommand;
import by.pressf.core.dto.orchestration.events.emailnotification.EmailOrderNotSentEvent;
import by.pressf.core.dto.orchestration.events.emailnotification.EmailOrderSentEvent;
import by.pressf.core.exceptions.NotRetryableException;
import by.pressf.core.exceptions.RetryableException;
import by.pressf.emailnotificationms.dao.entity.EventEntity;
import by.pressf.emailnotificationms.dao.repository.EventRepository;
import by.pressf.emailnotificationms.service.EmailService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.core.env.Environment;
import org.springframework.dao.DataAccessException;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.mail.MailException;
import org.springframework.mail.MailSendException;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
@KafkaListener(topics = "${email-notification.commands.topic.name}", groupId = "email-ms")
public class EmailCommandsHandler {
    private final Environment env;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final EmailService emailService;
    private final EventRepository eventRepository;

    @KafkaHandler
    @Transactional("transactionManager")
    public void handleCommand(@Payload SendEmailOrderCommand command,
                              @Header("messageId") String messageId) {
        try {
            log.info("The SendEmailOrderCommand command from the email-notification-commands topic has been received");

            EventEntity processedEvent = eventRepository.findByMessageId(messageId);
            if (processedEvent != null) {
                log.info("The SendEmailOrderCommand message with messageId={} has already been processed", messageId);
                return;
            }

            emailService.sendEmail(command.email(), command.subject(), command.body());
            log.info("The email was successfully delivered to the post office {}", command.email());

            EmailOrderSentEvent event = new EmailOrderSentEvent(command.orderId());
            ProducerRecord<String, Object> record =
                    new ProducerRecord<>(
                            env.getRequiredProperty("successful-events.topic.name"),
                            command.orderId().toString(),
                            event
                    );
            record.headers().add("messageId", UUID.randomUUID().toString().getBytes());

            kafkaTemplate.send(record);
            log.info("The EmailOrderSentEvent message was sent to the successful-events topic.");

            eventRepository.save(EventEntity.builder()
                    .messageId(messageId)
                    .build());
            log.info("The SendEmailOrderCommand message with messageId={} has been processed", messageId);
        } catch (MailSendException e) {
            log.error(e.getMessage());

            EmailOrderNotSentEvent failedEvent = new EmailOrderNotSentEvent(command.orderId());

            throw new RetryableException(e, env.getRequiredProperty("errors-successful-events.topic.name"),
                    command.orderId(), failedEvent);
        } catch (MailException | DataAccessException e) {
            log.error(e.getMessage());

            EmailOrderNotSentEvent failedEvent = new EmailOrderNotSentEvent(command.orderId());

            throw new NotRetryableException(e, env.getRequiredProperty("errors-successful-events.topic.name"),
                    command.orderId(), failedEvent);
        }
    }
}
