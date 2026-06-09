package by.pressf.orderms.saga.handler;

import by.pressf.core.dto.orchestration.commands.order.RejectOrderCommand;
import by.pressf.core.dto.orchestration.commands.payment.RefundPaymentCommand;
import by.pressf.core.dto.orchestration.commands.product.CancelProductReservationCommand;
import by.pressf.core.dto.orchestration.commands.user.CancelUserBalanceDebitCommand;
import by.pressf.core.dto.orchestration.events.order.OrderCompletionFailedEvent;
import by.pressf.core.dto.orchestration.events.payment.PaymentChargeFailedEvent;
import by.pressf.core.dto.orchestration.events.product.ProductReservationFailedEvent;
import by.pressf.core.dto.orchestration.events.user.UserBalanceDebitFailedEvent;
import by.pressf.orderms.dao.entity.status.OrderHistoryStatus;
import by.pressf.orderms.saga.publisher.KafkaCommandPublisher;
import by.pressf.orderms.service.IdempotencyService;
import by.pressf.orderms.service.OrderHistoryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NonNull;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Objects;

@Slf4j
@Component
@RequiredArgsConstructor
public class CompensationSagaHandler {
    private final IdempotencyService idempotencyService;
    private final OrderHistoryService orderHistoryService;
    private final KafkaCommandPublisher kafkaCommandPublisher;

    @Transactional("transactionManager")
    public void handleProductReservationFailedEvent(@NonNull ProductReservationFailedEvent event,
                                                    @NonNull String messageId) {
        Objects.requireNonNull(event);
        Objects.requireNonNull(messageId);

        idempotencyService.idempotenceCheck(messageId, event.getClass().getSimpleName());

        orderHistoryService.createHistoryLog(event.orderId(), OrderHistoryStatus.FAIL,
                "ProductMS: the product could not be booked; for more information, see the logs.");

        RejectOrderCommand command = new RejectOrderCommand(
                event.orderId(),
                event.username()
        );
        kafkaCommandPublisher.sendRejectOrderCommand(command.orderId().toString(), command);

        idempotencyService.saveIdempotentKey(messageId, event.getClass().getSimpleName());
    }

    @Transactional("transactionManager")
    public void handlePaymentChargeFailedEvent(@NonNull PaymentChargeFailedEvent event,
                                               @NonNull String messageId) {
        Objects.requireNonNull(event);
        Objects.requireNonNull(messageId);

        idempotencyService.idempotenceCheck(messageId, event.getClass().getSimpleName());

        orderHistoryService.createHistoryLog(event.orderId(), OrderHistoryStatus.FAIL,
                "PaymentMS: couldn't pay for the product; for more information, see the logs.");

        CancelProductReservationCommand command = new CancelProductReservationCommand(
                event.orderId(),
                event.username()
        );
        kafkaCommandPublisher.sendCancelProductReservationCommand(command.orderId().toString(), command);

        idempotencyService.saveIdempotentKey(messageId, event.getClass().getSimpleName());
    }

    @Transactional("transactionManager")
    public void handleUserBalanceDebitFailedEvent(@NonNull UserBalanceDebitFailedEvent event,
                                                  @NonNull String messageId) {
        Objects.requireNonNull(event);
        Objects.requireNonNull(messageId);

        idempotencyService.idempotenceCheck(messageId, event.getClass().getSimpleName());

        orderHistoryService.createHistoryLog(event.orderId(), OrderHistoryStatus.FAIL,
                "UserMS: couldn't change user's balance; for more information, see the logs.");

        RefundPaymentCommand command = new RefundPaymentCommand(
                event.orderId(),
                event.username()
        );
        kafkaCommandPublisher.sendRefundPaymentCommand(command.orderId().toString(), command);

        idempotencyService.saveIdempotentKey(messageId, event.getClass().getSimpleName());
    }

    @Transactional("transactionManager")
    public void handleOrderCompletionFailedEvent(@NonNull OrderCompletionFailedEvent event,
                                                 @NonNull String messageId) {
        Objects.requireNonNull(event);
        Objects.requireNonNull(messageId);

        idempotencyService.idempotenceCheck(messageId, event.getClass().getSimpleName());

        orderHistoryService.createHistoryLog(event.orderId(), OrderHistoryStatus.FAIL,
                "OrderMS: couldn't change the order status to APPROVED; for more information, see the logs.");

        CancelUserBalanceDebitCommand command = new CancelUserBalanceDebitCommand(
                event.orderId(),
                event.userId(),
                event.username(),
                event.amount()
        );
        kafkaCommandPublisher.sendCancelUserBalanceDebitCommand(command.orderId().toString(), command);

        idempotencyService.saveIdempotentKey(messageId, event.getClass().getSimpleName());
    }
}
