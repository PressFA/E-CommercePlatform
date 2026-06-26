package by.pressf.userms.kafka.handler;

import by.pressf.core.dto.orchestration.commands.user.CancelUserBalanceDebitCommand;
import by.pressf.core.dto.orchestration.commands.user.DebitUserBalanceCommand;
import by.pressf.core.dto.orchestration.events.user.UserBalanceDebitCanceledEvent;
import by.pressf.core.dto.orchestration.events.user.UserBalanceDebitedEvent;
import by.pressf.userms.dto.internal.UserBalanceRequest;
import by.pressf.userms.kafka.publisher.KafkaEventPublisher;
import by.pressf.userms.service.IdempotencyService;
import by.pressf.userms.service.UserService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NullMarked;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Component
@NullMarked
@RequiredArgsConstructor
public class UserCommandsHandler {
    private final UserService userService;
    private final KafkaEventPublisher kafkaEventPublisher;
    private final IdempotencyService idempotencyService;

    @Transactional("transactionManager")
    public void handleDebitUserBalanceCommand(DebitUserBalanceCommand command, String messageId) {
        idempotencyService.idempotenceCheck(messageId, command.getClass().getSimpleName());

        userService.debitUserBalance(new UserBalanceRequest(command.userId(), command.amount()));
        log.info("Debiting a user's balance with id {}", command.userId());

        UserBalanceDebitedEvent event = new UserBalanceDebitedEvent(
                command.orderId(),
                command.userId(),
                command.username(),
                command.amount()
        );

        kafkaEventPublisher.sendMessageUserBalanceDebitedEvent(command.orderId().toString(), event);

        idempotencyService.saveIdempotentKey(messageId, command.getClass().getSimpleName());
    }

    @Transactional("transactionManager")
    public void handleCancelUserBalanceDebitCommand(CancelUserBalanceDebitCommand command, String messageId) {
        idempotencyService.idempotenceCheck(messageId, command.getClass().getSimpleName());

        userService.creditUserBalance(new UserBalanceRequest(command.userId(), command.amount()));
        log.info("The balance of the user with ID {} has been topped up", command.userId());

        UserBalanceDebitCanceledEvent event = new UserBalanceDebitCanceledEvent(
                command.orderId(),
                command.username()
        );

        kafkaEventPublisher.sendMessageUserBalanceDebitCanceledEvent(command.orderId().toString(), event);

        idempotencyService.saveIdempotentKey(messageId, command.getClass().getSimpleName());
    }
}
