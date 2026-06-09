package by.pressf.orderms.saga.handler;

import by.pressf.core.dto.orchestration.commands.emailnotification.SendEmailOrderCommand;
import by.pressf.core.dto.orchestration.commands.order.RejectOrderCommand;
import by.pressf.core.dto.orchestration.commands.payment.RefundPaymentCommand;
import by.pressf.core.dto.orchestration.commands.product.CancelProductReservationCommand;
import by.pressf.core.dto.orchestration.events.order.OrderRejectedEvent;
import by.pressf.core.dto.orchestration.events.payment.PaymentRefundedEvent;
import by.pressf.core.dto.orchestration.events.product.ProductReservationCanceledEvent;
import by.pressf.core.dto.orchestration.events.user.UserBalanceDebitCanceledEvent;
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
public class RollbackSagaHandler {
    private final IdempotencyService idempotencyService;
    private final OrderHistoryService orderHistoryService;
    private final KafkaCommandPublisher kafkaCommandPublisher;

    @Transactional("transactionManager")
    public void handleOrderRejectedEvent(@NonNull OrderRejectedEvent event,
                                         @NonNull String messageId) {
        Objects.requireNonNull(event);
        Objects.requireNonNull(messageId);

        idempotencyService.idempotenceCheck(messageId, event.getClass().getSimpleName());

        orderHistoryService.createHistoryLog(event.orderId(), OrderHistoryStatus.SUCCESS,
                "OrderMS: the order status has been successfully changed to REJECTED");

        String bodyStr1 = "Hi there!\n";
        String bodyStr2 = "An error occurred under the ID " + event.orderId() + " during the checkout process. Please try to place an order later.";
        SendEmailOrderCommand command = new SendEmailOrderCommand(
                event.username(),
                "Couldn't place an order!",
                bodyStr1 + bodyStr2,
                event.orderId()
        );
        kafkaCommandPublisher.sendSendEmailOrderCommand(command.orderId().toString(), command);

        orderHistoryService.createHistoryLog(event.orderId(), OrderHistoryStatus.SUCCESS,
                "OrderSaga: saga has completed its work");

        idempotencyService.saveIdempotentKey(messageId, event.getClass().getSimpleName());
    }

    @Transactional("transactionManager")
    public void handleProductReservationCanceledEvent(@NonNull ProductReservationCanceledEvent event,
                                                      @NonNull String messageId) {
        Objects.requireNonNull(event);
        Objects.requireNonNull(messageId);

        idempotencyService.idempotenceCheck(messageId, event.getClass().getSimpleName());

        orderHistoryService.createHistoryLog(event.orderId(), OrderHistoryStatus.SUCCESS,
                "ProductMS: reservation has been successfully lifted from the product");

        RejectOrderCommand command = new RejectOrderCommand(event.orderId(), event.username());
        kafkaCommandPublisher.sendRejectOrderCommand(command.orderId().toString(), command);

        idempotencyService.saveIdempotentKey(messageId, event.getClass().getSimpleName());
    }

    @Transactional("transactionManager")
    public void handlePaymentRefundedEvent(@NonNull PaymentRefundedEvent event,
                                           @NonNull String messageId) {
        Objects.requireNonNull(event);
        Objects.requireNonNull(messageId);

        idempotencyService.idempotenceCheck(messageId, event.getClass().getSimpleName());

        orderHistoryService.createHistoryLog(event.orderId(), OrderHistoryStatus.SUCCESS,
                "PaymentMS: money has been successfully refunded from the payment");

        CancelProductReservationCommand command = new CancelProductReservationCommand(event.orderId(), event.username());
        kafkaCommandPublisher.sendCancelProductReservationCommand(command.orderId().toString(), command);

        idempotencyService.saveIdempotentKey(messageId, event.getClass().getSimpleName());
    }

    @Transactional("transactionManager")
    public void handleUserBalanceDebitCanceledEvent(@NonNull UserBalanceDebitCanceledEvent event,
                                                    @NonNull String messageId) {
        Objects.requireNonNull(event);
        Objects.requireNonNull(messageId);

        idempotencyService.idempotenceCheck(messageId, event.getClass().getSimpleName());

        orderHistoryService.createHistoryLog(event.orderId(), OrderHistoryStatus.SUCCESS,
                "UserMS: the money was successfully refunded to the user");

        RefundPaymentCommand command = new RefundPaymentCommand(event.orderId(), event.username());
        kafkaCommandPublisher.sendRefundPaymentCommand(command.orderId().toString(), command);

        idempotencyService.saveIdempotentKey(messageId, event.getClass().getSimpleName());
    }
}
