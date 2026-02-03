package by.pressf.userms.service.handler;

import by.pressf.core.dto.choreography.events.BalanceTopUpFailedEvent;
import by.pressf.core.dto.choreography.events.UserBalanceCreditFailedEvent;
import by.pressf.core.exceptions.NotRetryableException;
import by.pressf.core.exceptions.RetryableException;
import by.pressf.userms.dao.entity.EventEntity;
import by.pressf.userms.dao.repository.EventRepository;
import by.pressf.userms.dto.UserBalanceRequest;
import by.pressf.userms.exception.UserNotFoundException;
import by.pressf.userms.service.UserService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.core.env.Environment;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
@KafkaListener(topics = "${r-user-w-payment.topic.name}", groupId = "user-ms")
public class RUserWPaymentEventsHandler {
    private final Environment env;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final UserService userService;
    private final EventRepository eventRepository;

    @KafkaHandler
    @Transactional("transactionManager")
    public void handle(@Payload UserBalanceCreditFailedEvent event,
                       @Header("messageId") String messageId) {
        try {
            log.warn("The UserBalanceCreditFailedEvent event from the r-user-w-payment-events topic has been received");

            EventEntity processedEvent = eventRepository.findByMessageId(messageId);
            if (processedEvent != null) {
                log.warn("The UserBalanceCreditFailedEvent message with messageId={} has already been processed", messageId);
                return;
            }

            userService.cancelTopUpUserBalance(new UserBalanceRequest(event.userId(), event.amount()));
            log.warn("Couldn't top up the user's balance with the {} ID: error on the part of the payment service", event.userId());

            String bodyStr1 = "Hi there!\n";
            String bodyStr2 = "Unfortunately, we couldn't process your payment of " + event.amount() + " due to a bank or provider error. Please try again in a few minutes.";
            BalanceTopUpFailedEvent newEvent = new BalanceTopUpFailedEvent(
                    event.email(),
                    "Balance Top-Up Failed",
                    bodyStr1 + bodyStr2
            );

            ProducerRecord<String, Object> record =
                    new ProducerRecord<>(
                            env.getRequiredProperty("r-email-w-user.topic.name"),
                            event.userId().toString(),
                            newEvent
                    );
            record.headers().add("messageId", UUID.randomUUID().toString().getBytes());

            kafkaTemplate.send(record);
            log.warn("The BalanceTopUpFailedEvent message was sent to the r-email-w-user-events topic");

            eventRepository.save(EventEntity.builder()
                    .messageId(messageId)
                    .build());
            log.warn("The UserBalanceCreditFailedEvent message with messageId={} has been processed", messageId);
        } catch (OptimisticLockingFailureException e) {
            log.error(e.getMessage());
            throw new RetryableException(e);
        } catch (UserNotFoundException | DataAccessException e) {
            log.error(e.getMessage());
            throw new NotRetryableException(e);
        }
    }
}
