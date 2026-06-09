package by.pressf.userms.kafka.listener;

import by.pressf.core.dto.orchestration.commands.user.CancelUserBalanceDebitCommand;
import by.pressf.core.dto.orchestration.commands.user.DebitUserBalanceCommand;
import by.pressf.core.dto.orchestration.events.user.UserBalanceDebitCancelFailedEvent;
import by.pressf.core.dto.orchestration.events.user.UserBalanceDebitFailedEvent;
import by.pressf.core.exceptions.DuplicateMessageException;
import by.pressf.core.exceptions.NotRetryableException;
import by.pressf.core.exceptions.RetryableException;
import by.pressf.userms.exception.InsufficientBalanceException;
import by.pressf.userms.exception.UserNotFoundException;
import by.pressf.userms.kafka.handler.UserCommandsHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.env.Environment;
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
@KafkaListener(topics = "${user.commands.topic.name}", groupId = "user-ms")
public class UserCommandsListener {
    private final Environment env;
    private final UserCommandsHandler handler;

    @KafkaHandler
    public void handleCommand(@Payload DebitUserBalanceCommand command,
                              @Header("messageId") String messageId) {
        try {
            log.info("The DebitUserBalanceCommand command from the user-commands topic has been received");

            handler.handleDebitUserBalanceCommand(command, messageId);
        } catch (DuplicateMessageException e) {
            log.warn(e.getMessage());
        } catch (OptimisticLockingFailureException e) {
            log.error(e.getMessage());

            UserBalanceDebitFailedEvent failedEvent = createDebitFailedEvent(command);

            throw new RetryableException(e, env.getRequiredProperty("errors-successful-events.topic.name"),
                    command.orderId(), failedEvent);
        } catch (UserNotFoundException | InsufficientBalanceException | DataAccessException e) {
            log.error(e.getMessage());

            UserBalanceDebitFailedEvent failedEvent = createDebitFailedEvent(command);

            throw new NotRetryableException(e, env.getRequiredProperty("errors-successful-events.topic.name"),
                    command.orderId(), failedEvent);
        }
    }

    @KafkaHandler
    public void handleCommand(@Payload CancelUserBalanceDebitCommand command,
                              @Header("messageId") String messageId) {
        try {
            log.info("The CancelUserBalanceDebitCommand command from the user-commands topic has been received");

            handler.handleCancelUserBalanceDebitCommand(command, messageId);
        } catch (DuplicateMessageException e) {
            log.warn(e.getMessage());
        } catch (OptimisticLockingFailureException e) {
            log.error(e.getMessage());

            UserBalanceDebitCancelFailedEvent failedEvent = createCancelFailedEvent(command);

            throw new RetryableException(e, env.getRequiredProperty("errors-compensating-events.topic.name"),
                    command.orderId(), failedEvent);
        } catch (UserNotFoundException | DataAccessException e) {
            log.error(e.getMessage());

            UserBalanceDebitCancelFailedEvent failedEvent = createCancelFailedEvent(command);

            throw new NotRetryableException(e, env.getRequiredProperty("errors-compensating-events.topic.name"),
                    command.orderId(), failedEvent);
        }
    }

    private UserBalanceDebitFailedEvent createDebitFailedEvent(DebitUserBalanceCommand command) {
        return new UserBalanceDebitFailedEvent(command.orderId(), command.username());
    }

    private UserBalanceDebitCancelFailedEvent createCancelFailedEvent(CancelUserBalanceDebitCommand command) {
        return new UserBalanceDebitCancelFailedEvent(command.orderId(), command.username());
    }
}
