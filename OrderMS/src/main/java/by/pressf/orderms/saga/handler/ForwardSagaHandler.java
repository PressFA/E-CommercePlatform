package by.pressf.orderms.saga.handler;

import by.pressf.core.dto.orchestration.commands.emailnotification.SendEmailOrderCommand;
import by.pressf.core.dto.orchestration.commands.order.ConfirmOrderCommand;
import by.pressf.core.dto.orchestration.commands.payment.ChargePaymentCommand;
import by.pressf.core.dto.orchestration.commands.product.ReserveProductCommand;
import by.pressf.core.dto.orchestration.commands.user.DebitUserBalanceCommand;
import by.pressf.core.dto.orchestration.events.order.OrderCompletedEvent;
import by.pressf.core.dto.orchestration.events.order.OrderCreatedEvent;
import by.pressf.core.dto.orchestration.events.payment.PaymentChargedEvent;
import by.pressf.core.dto.orchestration.events.product.ProductReservedEvent;
import by.pressf.core.dto.orchestration.events.user.UserBalanceDebitedEvent;
import by.pressf.orderms.dao.entity.status.OrderHistoryStatus;
import by.pressf.orderms.saga.publisher.KafkaCommandPublisher;
import by.pressf.orderms.service.IdempotencyService;
import by.pressf.orderms.service.OrderHistoryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NonNull;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Date;
import java.util.Objects;

@Slf4j
@Component
@RequiredArgsConstructor
public class ForwardSagaHandler {
    private final IdempotencyService idempotencyService;
    private final OrderHistoryService orderHistoryService;
    private final KafkaCommandPublisher kafkaCommandPublisher;

    @Transactional("transactionManager")
    public void handleOrderCreatedEvent(@NonNull OrderCreatedEvent event,
                                        @NonNull String messageId) {
        Objects.requireNonNull(event);
        Objects.requireNonNull(messageId);

        idempotencyService.idempotenceCheck(messageId, event.getClass().getSimpleName());

        orderHistoryService.createHistoryLog(event.orderId(), OrderHistoryStatus.SUCCESS,
                "OrderMS: starting to form an order for the user");

        ReserveProductCommand command = new ReserveProductCommand(
                event.orderId(),
                event.productId(),
                event.userId(),
                event.username(),
                event.quantity()
        );
        kafkaCommandPublisher.sendReserveProductCommand(command.orderId().toString(), command);

        idempotencyService.saveIdempotentKey(messageId, event.getClass().getSimpleName());
    }

    @Transactional("transactionManager")
    public void handleProductReservedEvent(@NonNull ProductReservedEvent event,
                                           @NonNull String messageId) {
        Objects.requireNonNull(event);
        Objects.requireNonNull(messageId);

        idempotencyService.idempotenceCheck(messageId, event.getClass().getSimpleName());

        orderHistoryService.createHistoryLog(event.orderId(), OrderHistoryStatus.SUCCESS,
                "ProductMS: the product is reserved");

        ChargePaymentCommand command = new ChargePaymentCommand(
                event.orderId(),
                event.userId(),
                event.username(),
                event.amount()
        );
        kafkaCommandPublisher.sendChargePaymentCommand(command.orderId().toString(), command);

        idempotencyService.saveIdempotentKey(messageId, event.getClass().getSimpleName());
    }

    @Transactional("transactionManager")
    public void handlePaymentChargedEvent(@NonNull PaymentChargedEvent event,
                                          @NonNull String messageId) {
        Objects.requireNonNull(event);
        Objects.requireNonNull(messageId);

        idempotencyService.idempotenceCheck(messageId, event.getClass().getSimpleName());

        orderHistoryService.createHistoryLog(event.orderId(), OrderHistoryStatus.SUCCESS,
                "PaymentMS: the payment was successful");

        DebitUserBalanceCommand command = new DebitUserBalanceCommand(
                event.orderId(),
                event.userId(),
                event.username(),
                event.amount()
        );
        kafkaCommandPublisher.sendDebitUserBalanceCommand(command.orderId().toString(), command);

        idempotencyService.saveIdempotentKey(messageId, event.getClass().getSimpleName());
    }

    @Transactional("transactionManager")
    public void handleUserBalanceDebitedEvent(@NonNull UserBalanceDebitedEvent event,
                                              @NonNull String messageId) {
        Objects.requireNonNull(event);
        Objects.requireNonNull(messageId);

        idempotencyService.idempotenceCheck(messageId, event.getClass().getSimpleName());

        orderHistoryService.createHistoryLog(event.orderId(), OrderHistoryStatus.SUCCESS,
                "UserMS: the user's balance has been successfully changed");

        ConfirmOrderCommand command = new ConfirmOrderCommand(
                event.orderId(),
                event.userId(),
                event.username(),
                event.amount()
        );
        kafkaCommandPublisher.sendConfirmOrderCommand(command.orderId().toString(), command);

        idempotencyService.saveIdempotentKey(messageId, event.getClass().getSimpleName());
    }

    @Transactional("transactionManager")
    public void handleOrderCompletedEvent(@NonNull OrderCompletedEvent event,
                                          @NonNull String messageId) {
        Objects.requireNonNull(event);
        Objects.requireNonNull(messageId);

        idempotencyService.idempotenceCheck(messageId, event.getClass().getSimpleName());

        orderHistoryService.createHistoryLog(event.orderId(), OrderHistoryStatus.SUCCESS,
                "OrderMS: The order status has been successfully changed to APPROVED");

        String bodyStr1 = "Hi there!\n";
        String bodyStr2 = "Your order under the ID " + event.orderId()
                + " has been successfully placed. Date of the order: " + new Date();
        SendEmailOrderCommand command = new SendEmailOrderCommand(
                event.username(),
                "The order has been placed!",
                bodyStr1 + bodyStr2,
                event.orderId()
        );
        kafkaCommandPublisher.sendSendEmailOrderCommand(command.orderId().toString(), command);

        orderHistoryService.createHistoryLog(event.orderId(), OrderHistoryStatus.SUCCESS,
                "OrderSaga: saga has completed its work");

        idempotencyService.saveIdempotentKey(messageId, event.getClass().getSimpleName());
    }
}
