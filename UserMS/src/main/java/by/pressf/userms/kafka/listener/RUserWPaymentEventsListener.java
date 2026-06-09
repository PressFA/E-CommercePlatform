package by.pressf.userms.kafka.listener;

import by.pressf.core.dto.choreography.events.UserBalanceCreditFailedEvent;
import by.pressf.core.exceptions.DuplicateMessageException;
import by.pressf.core.exceptions.NotRetryableException;
import by.pressf.core.exceptions.RetryableException;
import by.pressf.userms.exception.UserNotFoundException;
import by.pressf.userms.kafka.handler.UserEventsHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
@KafkaListener(topics = "${r-user-w-payment.topic.name}", groupId = "user-ms")
public class RUserWPaymentEventsListener {
    private final UserEventsHandler handler;

    @KafkaHandler
    public void handle(@Payload UserBalanceCreditFailedEvent event,
                       @Header("messageId") String messageId) {
        try {
            log.warn("The UserBalanceCreditFailedEvent event from the r-user-w-payment-events topic has been received");

            handler.handleUserBalanceCreditFailedEvent(event, messageId);
        } catch (DuplicateMessageException e) {
            log.warn(e.getMessage());
        } catch (OptimisticLockingFailureException e) {
            log.error(e.getMessage());
            throw new RetryableException(e);
        } catch (UserNotFoundException | DataAccessException e) {
            log.error(e.getMessage());
            throw new NotRetryableException(e);
        }
    }
}
