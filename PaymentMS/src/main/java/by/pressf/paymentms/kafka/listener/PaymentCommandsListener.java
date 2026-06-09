package by.pressf.paymentms.kafka.listener;

import by.pressf.core.dto.orchestration.commands.payment.RefundPaymentCommand;
import by.pressf.core.dto.orchestration.commands.payment.ChargePaymentCommand;
import by.pressf.core.dto.orchestration.events.payment.PaymentChargeFailedEvent;
import by.pressf.core.dto.orchestration.events.payment.PaymentRefundFailedEvent;
import by.pressf.core.exceptions.DuplicateMessageException;
import by.pressf.core.exceptions.NotRetryableException;
import by.pressf.core.exceptions.RetryableException;
import by.pressf.paymentms.exception.PaymentFailedException;
import by.pressf.paymentms.exception.PaymentNotFoundByOrderIdException;
import by.pressf.paymentms.kafka.handler.PaymentCommandsHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.env.Environment;
import org.springframework.dao.DataAccessException;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
@KafkaListener(topics = "${payment.commands.topic.name}", groupId = "payment-ms")
public class PaymentCommandsListener {
    private final Environment env;
    private final PaymentCommandsHandler handler;

    @KafkaHandler
    public void handleCommand(@Payload ChargePaymentCommand command,
                              @Header("messageId") String messageId) {
        try {
            log.info("The ChargePaymentCommand command from the payment-commands topic has been received");

            handler.handleChargePaymentCommand(command, messageId);
        } catch (DuplicateMessageException e) {
            log.warn(e.getMessage());
        } catch (PaymentFailedException e) {
            log.error(e.getMessage());

            PaymentChargeFailedEvent failedEvent = createChargeFailedEvent(command);

            throw handleStripeException(e, env.getRequiredProperty("errors-successful-events.topic.name"),
                    command.orderId(), failedEvent);
        } catch (NullPointerException | PaymentNotFoundByOrderIdException | DataAccessException e) {
            log.error(e.getMessage());

            PaymentChargeFailedEvent failedEvent = createChargeFailedEvent(command);

            throw new NotRetryableException(e, env.getRequiredProperty("errors-successful-events.topic.name"),
                    command.orderId(), failedEvent);
        }
    }

    @KafkaHandler
    public void handleCommand(@Payload RefundPaymentCommand command,
                              @Header("messageId") String messageId) {
        try {
            log.warn("The RefundPaymentCommand command from the payment-commands topic has been received");

            handler.handleRefundPaymentCommand(command, messageId);
        } catch (DuplicateMessageException e) {
            log.warn(e.getMessage());
        } catch (PaymentFailedException e) {
            log.error(e.getMessage());

            PaymentRefundFailedEvent failedEvent = createRefundFailedEvent(command);

            throw handleStripeException(e, env.getRequiredProperty("errors-compensating-events.topic.name"),
                    command.orderId(), failedEvent);
        } catch (NullPointerException | PaymentNotFoundByOrderIdException | DataAccessException e) {
            log.error(e.getMessage());

            PaymentRefundFailedEvent failedEvent = createRefundFailedEvent(command);

            throw new NotRetryableException(e, env.getRequiredProperty("errors-compensating-events.topic.name"),
                    command.orderId(), failedEvent);
        }
    }

    private <T> RuntimeException handleStripeException(PaymentFailedException e, String topic, UUID orderId, T failedEvent) {
        RuntimeException returnEx;
        switch (e.getStatusCode()) {
            case 400, 401, 402, 403, 404 ->
                    returnEx = new RetryableException(e, topic, orderId, failedEvent);
            default -> {
                if (e.getStatusCode() == 0) log.error("Error on the part of our service");
                returnEx = new NotRetryableException(e, topic, orderId, failedEvent);
            }
        }
        return returnEx;
    }

    private PaymentChargeFailedEvent createChargeFailedEvent(ChargePaymentCommand command) {
        return new PaymentChargeFailedEvent(command.orderId(), command.username());
    }

    private PaymentRefundFailedEvent createRefundFailedEvent(RefundPaymentCommand command) {
        return new PaymentRefundFailedEvent(command.orderId(), command.username());
    }
}
