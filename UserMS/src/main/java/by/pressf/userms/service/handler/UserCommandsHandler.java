package by.pressf.userms.service.handler;

import by.pressf.core.dto.commands.user.CancelUserBalanceDebitCommand;
import by.pressf.core.dto.commands.user.DebitUserBalanceCommand;
import by.pressf.core.dto.events.user.UserBalanceDebitCancelFailedEvent;
import by.pressf.core.dto.events.user.UserBalanceDebitCanceledEvent;
import by.pressf.core.dto.events.user.UserBalanceDebitFailedEvent;
import by.pressf.core.dto.events.user.UserBalanceDebitedEvent;
import by.pressf.core.exceptions.NotRetryableException;
import by.pressf.core.exceptions.RetryableException;
import by.pressf.userms.dao.entity.EventEntity;
import by.pressf.userms.dao.repository.EventRepository;
import by.pressf.userms.dto.UserBalanceRequest;
import by.pressf.userms.exception.InsufficientBalanceException;
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
@KafkaListener(topics = "${user.commands.topic.name}", groupId = "user-ms")
public class UserCommandsHandler {
    private final Environment env;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final UserService userService;
    private final EventRepository eventRepository;

    @KafkaHandler
    @Transactional("transactionManager")
    public void handleCommand(@Payload DebitUserBalanceCommand command,
                              @Header("messageId") String messageId) {
        try {
            log.info("The DebitUserBalanceCommand command from the user-commands topic has been received");

            EventEntity processedEvent = eventRepository.findByMessageId(messageId);
            if (processedEvent != null) {
                log.info("The DebitUserBalanceCommand message with messageId={} has already been processed", messageId);
                return;
            }

            userService.debitUserBalance(new UserBalanceRequest(command.userId(), command.amount()));
            log.info("Debiting a user's balance with id {}", command.userId());

            UserBalanceDebitedEvent event = new UserBalanceDebitedEvent(
                    command.orderId(),
                    command.userId(),
                    command.username(),
                    command.amount()
            );

            ProducerRecord<String, Object> record =
                    new ProducerRecord<>(
                            env.getRequiredProperty("user.events.topic.name"),
                            command.orderId().toString(),
                            event
                    );
            record.headers().add("messageId", UUID.randomUUID().toString().getBytes());

            kafkaTemplate.send(record);
            log.info("The UserBalanceDebitedEvent message was sent to the user-events topic");

            eventRepository.save(EventEntity.builder()
                    .messageId(messageId)
                    .build());
            log.info("The DebitUserBalanceCommand message with messageId={} has been processed", messageId);
        } catch (OptimisticLockingFailureException e) {
            log.error(e.getMessage());

            UserBalanceDebitFailedEvent failedEvent = createDebitFailedEvent(command);

            throw new RetryableException(e, env.getRequiredProperty("user.events.topic.name"),
                    command.orderId(), failedEvent);
        } catch (UserNotFoundException | InsufficientBalanceException | DataAccessException e) {
            log.error(e.getMessage());

            UserBalanceDebitFailedEvent failedEvent = createDebitFailedEvent(command);

            throw new NotRetryableException(e, env.getRequiredProperty("user.events.topic.name"),
                    command.orderId(), failedEvent);
        }
    }

    @KafkaHandler
    @Transactional("transactionManager")
    public void handleCommand(@Payload CancelUserBalanceDebitCommand command,
                              @Header("messageId") String messageId) {
        try {
            log.info("The CancelUserBalanceDebitCommand command from the user-commands topic has been received");

            EventEntity processedEvent = eventRepository.findByMessageId(messageId);
            if (processedEvent != null) {
                log.info("The CancelUserBalanceDebitCommand message with messageId={} has already been processed", messageId);
                return;
            }

            userService.creditUserBalance(new UserBalanceRequest(command.userId(), command.amount()));
            log.info("The balance of the user with ID {} has been topped up", command.userId());

            UserBalanceDebitCanceledEvent event = new UserBalanceDebitCanceledEvent(command.orderId(), command.username());
            ProducerRecord<String, Object> record =
                    new ProducerRecord<>(
                            env.getRequiredProperty("user.events.topic.name"),
                            command.orderId().toString(),
                            event
                    );
            record.headers().add("messageId", UUID.randomUUID().toString().getBytes());

            kafkaTemplate.send(record);
            log.info("The UserBalanceDebitCanceledEvent message was sent to the user-events topic");

            eventRepository.save(EventEntity.builder()
                    .messageId(messageId)
                    .build());
            log.info("The CancelUserBalanceDebitCommand message with messageId={} has been processed", messageId);
        } catch (OptimisticLockingFailureException e) {
            log.error(e.getMessage());

            UserBalanceDebitCancelFailedEvent failedEvent = createCancelFailedEvent(command);

            throw new RetryableException(e, env.getRequiredProperty("user.events.topic.name"),
                    command.orderId(), failedEvent);
        } catch (UserNotFoundException | DataAccessException e) {
            log.error(e.getMessage());

            UserBalanceDebitCancelFailedEvent failedEvent = createCancelFailedEvent(command);

            throw new NotRetryableException(e, env.getRequiredProperty("user.events.topic.name"),
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
