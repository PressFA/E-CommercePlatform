package by.pressf.userms.kafka.handler;

import by.pressf.core.dto.choreography.events.BalanceTopUpFailedEvent;
import by.pressf.core.dto.choreography.events.UserBalanceCreditFailedEvent;
import by.pressf.userms.dto.internal.UserBalanceRequest;
import by.pressf.userms.kafka.publisher.KafkaEventPublisher;
import by.pressf.userms.service.IdempotencyService;
import by.pressf.userms.service.UserService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NonNull;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Objects;

@Slf4j
@Component
@RequiredArgsConstructor
public class UserEventsHandler {
    private final UserService userService;
    private final KafkaEventPublisher kafkaEventPublisher;
    private final IdempotencyService idempotencyService;

    @Transactional("transactionManager")
    public void handleUserBalanceCreditFailedEvent(@NonNull UserBalanceCreditFailedEvent event,
                                                   @NonNull String messageId) {
        Objects.requireNonNull(event);
        Objects.requireNonNull(messageId);

        idempotencyService.idempotenceCheck(messageId, event.getClass().getSimpleName());

        userService.cancelTopUpUserBalance(new UserBalanceRequest(event.userId(), event.amount()));
        log.warn("Couldn't top up the user's balance with the {} ID: error on the part of the payment service", event.userId());

        String bodyStr1 = "Hi there!\n";
        String bodyStr2 = "Unfortunately, we couldn't process your payment of " + event.amount() + " due to a bank or provider error. Please try again in a few minutes.";
        BalanceTopUpFailedEvent newEvent = new BalanceTopUpFailedEvent(
                event.email(),
                "Balance Top-Up Failed",
                bodyStr1 + bodyStr2
        );

        kafkaEventPublisher.sendMessageBalanceTopUpFailedEvent(event.userId().toString(), newEvent);

        idempotencyService.saveIdempotentKey(messageId, newEvent.getClass().getSimpleName());
    }
}
