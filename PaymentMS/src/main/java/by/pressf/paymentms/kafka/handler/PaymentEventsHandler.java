package by.pressf.paymentms.kafka.handler;

import by.pressf.core.dto.choreography.events.BalanceTopUpCompletedEvent;
import by.pressf.core.dto.choreography.events.UserBalanceCreditedEvent;
import by.pressf.paymentms.dto.UserBalanceRequest;
import by.pressf.paymentms.kafka.publisher.KafkaEventPublisher;
import by.pressf.paymentms.service.IdempotencyService;
import by.pressf.paymentms.service.PaymentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NullMarked;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Component
@NullMarked
@RequiredArgsConstructor
public class PaymentEventsHandler {
    private final PaymentService paymentService;
    private final KafkaEventPublisher kafkaEventPublisher;
    private final IdempotencyService idempotencyService;

    @Transactional("transactionManager")
    public void handleUserBalanceCreditedEvent(UserBalanceCreditedEvent event, String messageId) {
        idempotencyService.idempotenceCheck(messageId, event.getClass().getSimpleName());

        paymentService.topUpBalance(new UserBalanceRequest(messageId, event.userId(), event.amount()));
        log.info("The bank approved the transaction. Replenishment of the balance for the user with the ID {} was successful", event.userId());

        String bodyStr1 = "Hi there!\n";
        String bodyStr2 = "Success! Your balance has been topped up by " + event.amount() + ".";
        BalanceTopUpCompletedEvent newEvent = new BalanceTopUpCompletedEvent(
                event.email(),
                "Balance Topped Up",
                bodyStr1 + bodyStr2
        );

        kafkaEventPublisher.sendMessageBalanceTopUpCompletedEvent(event.userId().toString(), newEvent);

        idempotencyService.saveIdempotentKey(messageId, event.getClass().getSimpleName());
    }
}
