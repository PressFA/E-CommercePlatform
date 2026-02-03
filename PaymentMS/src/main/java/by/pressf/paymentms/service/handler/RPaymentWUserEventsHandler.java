package by.pressf.paymentms.service.handler;

import by.pressf.core.dto.choreography.events.BalanceTopUpCompletedEvent;
import by.pressf.core.dto.choreography.events.UserBalanceCreditFailedEvent;
import by.pressf.core.dto.choreography.events.UserBalanceCreditedEvent;
import by.pressf.core.exceptions.NotRetryableException;
import by.pressf.core.exceptions.RetryableException;
import by.pressf.paymentms.dao.entity.EventEntity;
import by.pressf.paymentms.dao.repository.EventRepository;
import by.pressf.paymentms.dto.UserBalanceRequest;
import by.pressf.paymentms.exception.PaymentFailedException;
import by.pressf.paymentms.service.PaymentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.core.env.Environment;
import org.springframework.dao.DataAccessException;
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
@KafkaListener(topics = "${r-payment-w-user.topic.name}", groupId = "payment-ms")
public class RPaymentWUserEventsHandler {
    private final Environment env;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final PaymentService paymentService;
    private final EventRepository eventRepository;

    @KafkaHandler
    @Transactional("transactionManager")
    public void handle(@Payload UserBalanceCreditedEvent event,
                       @Header("messageId") String messageId) {
        try {
            log.info("The UserBalanceCreditedEvent event from the r-payment-w-user-events topic has been received");

            EventEntity processedEvent = eventRepository.findByMessageId(messageId);
            if (processedEvent != null) {
                log.info("The UserBalanceCreditedEvent message with messageId={} has already been processed", messageId);
                return;
            }

            paymentService.topUpBalance(new UserBalanceRequest(messageId, event.userId(), event.amount()));
            log.info("The bank approved the transaction. Replenishment of the balance for the user with the ID {} was successful", event.userId());

            String bodyStr1 = "Hi there!\n";
            String bodyStr2 = "Success! Your balance has been topped up by " + event.amount() + ".";
            BalanceTopUpCompletedEvent newEvent = new BalanceTopUpCompletedEvent(
                    event.email(),
                    "Balance Topped Up",
                    bodyStr1 + bodyStr2
            );

            ProducerRecord<String, Object> record =
                    new ProducerRecord<>(
                            env.getRequiredProperty("r-email-w-payment.topic.name"),
                            event.userId().toString(),
                            newEvent
                    );
            record.headers().add("messageId", UUID.randomUUID().toString().getBytes());

            kafkaTemplate.send(record);
            log.info("The BalanceTopUpCompletedEvent message was sent to the r-email-w-payment-events topic");

            eventRepository.save(EventEntity.builder()
                    .messageId(messageId)
                    .build());
            log.info("The UserBalanceCreditedEvent message with messageId={} has been processed", messageId);
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
