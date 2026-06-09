package by.pressf.paymentms.kafka.listener;

import by.pressf.core.dto.choreography.events.UserBalanceCreditFailedEvent;
import by.pressf.core.dto.choreography.events.UserBalanceCreditedEvent;
import by.pressf.core.exceptions.DuplicateMessageException;
import by.pressf.core.exceptions.NotRetryableException;
import by.pressf.core.exceptions.RetryableException;
import by.pressf.paymentms.exception.PaymentFailedException;
import by.pressf.paymentms.kafka.handler.PaymentEventsHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.env.Environment;
import org.springframework.dao.DataAccessException;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
@KafkaListener(topics = "${r-payment-w-user.topic.name}", groupId = "payment-ms")
public class RPaymentWUserEventsListener {
    private final Environment env;
    private final PaymentEventsHandler handler;

    @KafkaHandler
    public void handle(@Payload UserBalanceCreditedEvent event,
                       @Header("messageId") String messageId) {
        try {
            log.info("The UserBalanceCreditedEvent event from the r-payment-w-user-events topic has been received");

            handler.handleUserBalanceCreditedEvent(event, messageId);
        } catch (DuplicateMessageException e) {
            log.warn(e.getMessage());
        } catch (PaymentFailedException e) {
            log.error(e.getMessage());

            UserBalanceCreditFailedEvent failedEvent = createFailedEvent(event);

            switch (e.getStatusCode()) {
                case 400, 401, 402, 403, 404 ->
                        throw new RetryableException(e, env.getRequiredProperty("r-user-w-payment.topic.name"),
                                event.userId(), failedEvent);
                default -> {
                    if (e.getStatusCode() == 0) log.error("Error on the part of our service");
                    throw new NotRetryableException(e, env.getRequiredProperty("r-user-w-payment.topic.name"),
                            event.userId(), failedEvent);
                }
            }
        } catch (DataAccessException e) {
            log.error(e.getMessage());

            UserBalanceCreditFailedEvent failedEvent = createFailedEvent(event);

            throw new NotRetryableException(e, env.getRequiredProperty("r-user-w-payment.topic.name"),
                    event.userId(), failedEvent);
        }
    }

    private UserBalanceCreditFailedEvent createFailedEvent(UserBalanceCreditedEvent event) {
        return new UserBalanceCreditFailedEvent(
                event.userId(),
                event.email(),
                event.amount()
        );
    }
}
