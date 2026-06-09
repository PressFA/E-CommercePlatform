package by.pressf.paymentms.kafka.handler;

import by.pressf.core.dto.orchestration.commands.payment.ChargePaymentCommand;
import by.pressf.core.dto.orchestration.commands.payment.RefundPaymentCommand;
import by.pressf.core.dto.orchestration.events.payment.PaymentChargedEvent;
import by.pressf.core.dto.orchestration.events.payment.PaymentRefundedEvent;
import by.pressf.paymentms.dto.CreateOrderPaymentRequest;
import by.pressf.paymentms.dto.RefundPaymentRequest;
import by.pressf.paymentms.kafka.publisher.KafkaEventPublisher;
import by.pressf.paymentms.service.IdempotencyService;
import by.pressf.paymentms.service.PaymentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NonNull;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Objects;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentCommandsHandler {
    private final PaymentService paymentService;
    private final KafkaEventPublisher kafkaEventPublisher;
    private final IdempotencyService idempotencyService;

    @Transactional("transactionManager")
    public void handleChargePaymentCommand(@NonNull ChargePaymentCommand command, @NonNull String messageId) {
        Objects.requireNonNull(command);
        Objects.requireNonNull(messageId);

        idempotencyService.idempotenceCheck(messageId, command.getClass().getSimpleName());

        CreateOrderPaymentRequest request = new CreateOrderPaymentRequest(
                messageId,
                command.orderId(),
                command.userId(),
                command.amount()
        );

        paymentService.createOrderPayment(request);
        log.info("The payment for the order with the {} ID was successful", command.orderId());

        PaymentChargedEvent event = new PaymentChargedEvent(
                command.orderId(),
                command.userId(),
                command.username(),
                command.amount()
        );

        kafkaEventPublisher.sendMessagePaymentChargedEvent(command.orderId().toString(), event);

        idempotencyService.saveIdempotentKey(messageId, command.getClass().getSimpleName());
    }

    @Transactional("transactionManager")
    public void handleRefundPaymentCommand(@NonNull RefundPaymentCommand command, @NonNull String messageId) {
        Objects.requireNonNull(command);
        Objects.requireNonNull(messageId);

        idempotencyService.idempotenceCheck(messageId, command.getClass().getSimpleName());

        RefundPaymentRequest refundPaymentRequest = new RefundPaymentRequest(messageId, command.orderId());

        paymentService.refundOrderPayment(refundPaymentRequest);
        log.warn("The refund for order ID {} has been successfully processed", command.orderId());

        PaymentRefundedEvent event = new PaymentRefundedEvent(command.orderId(), command.username());

        kafkaEventPublisher.sendMessagePaymentRefundedEvent(command.orderId().toString(), event);

        idempotencyService.saveIdempotentKey(messageId, command.getClass().getSimpleName());
    }
}
