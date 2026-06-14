package by.pressf.orderms.saga.handler;

import by.pressf.core.dto.orchestration.commands.emailnotification.SendEmailOrderCommand;
import by.pressf.core.dto.orchestration.commands.order.RejectOrderCommand;
import by.pressf.core.dto.orchestration.commands.payment.RefundPaymentCommand;
import by.pressf.core.dto.orchestration.commands.product.CancelProductReservationCommand;
import by.pressf.core.dto.orchestration.events.order.OrderRejectionFailedEvent;
import by.pressf.core.dto.orchestration.events.payment.PaymentRefundFailedEvent;
import by.pressf.core.dto.orchestration.events.product.ProductReservationCancelFailedEvent;
import by.pressf.core.dto.orchestration.events.user.UserBalanceDebitCancelFailedEvent;
import by.pressf.orderms.dao.entity.status.OrderHistoryStatus;
import by.pressf.orderms.saga.publisher.KafkaCommandPublisher;
import by.pressf.orderms.service.IdempotencyService;
import by.pressf.orderms.service.OrderHistoryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NullMarked;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Component
@NullMarked
@RequiredArgsConstructor
public class CriticalAuditSagaHandler {
    private final IdempotencyService idempotencyService;
    private final OrderHistoryService orderHistoryService;
    private final KafkaCommandPublisher kafkaCommandPublisher;

    @Transactional("transactionManager")
    public void handleOrderRejectionFailedEvent(OrderRejectionFailedEvent event, String messageId) {
        idempotencyService.idempotenceCheck(messageId, event.getClass().getSimpleName());

        log.error("An error occurred while compensating for a transaction in the OrderMS for an order with the ID {}",
                event.orderId());

        orderHistoryService.createHistoryLog(event.orderId(), OrderHistoryStatus.FAIL,
                "OrderMS(Warning!!!): an error occurred while compensating for a transaction; for more information, see the logs.");

        String bodyStr1 = "Hi there!\n";
        String bodyStr2 = "An error occurred under the ID " + event.orderId() + " during the checkout process. Please try to place an order later.";
        SendEmailOrderCommand command = new SendEmailOrderCommand(
                event.username(),
                "Couldn't place an order!",
                bodyStr1 + bodyStr2,
                event.orderId()
        );

        kafkaCommandPublisher.sendSendEmailOrderCommand(command.orderId().toString(), command);

        idempotencyService.saveIdempotentKey(messageId, event.getClass().getSimpleName());
    }

    @Transactional("transactionManager")
    public void handleProductReservationCancelFailedEvent(ProductReservationCancelFailedEvent event, String messageId) {
        idempotencyService.idempotenceCheck(messageId, event.getClass().getSimpleName());

        log.error("An error occurred while compensating for a transaction in the ProductMS for an order with the ID {}",
                event.orderId());

        orderHistoryService.createHistoryLog(event.orderId(), OrderHistoryStatus.FAIL,
                "ProductMS(Warning!!!): an error occurred while compensating for a transaction; for more information, see the logs.");

        RejectOrderCommand command = new RejectOrderCommand(event.orderId(), event.username());
        kafkaCommandPublisher.sendRejectOrderCommand(command.orderId().toString(), command);

        idempotencyService.saveIdempotentKey(messageId, event.getClass().getSimpleName());
    }

    @Transactional("transactionManager")
    public void handlePaymentRefundFailedEvent(PaymentRefundFailedEvent event, String messageId) {
        idempotencyService.idempotenceCheck(messageId, event.getClass().getSimpleName());

        log.error("An error occurred while compensating for a transaction in the PaymentMS for an order with the ID {}",
                event.orderId());

        orderHistoryService.createHistoryLog(event.orderId(), OrderHistoryStatus.FAIL,
                "PaymentMS(Warning!!!): an error occurred while compensating for a transaction; for more information, see the logs.");

        CancelProductReservationCommand command = new CancelProductReservationCommand(event.orderId(), event.username());
        kafkaCommandPublisher.sendCancelProductReservationCommand(command.orderId().toString(), command);

        idempotencyService.saveIdempotentKey(messageId, event.getClass().getSimpleName());
    }

    @Transactional("transactionManager")
    public void handleUserBalanceDebitCancelFailedEvent(UserBalanceDebitCancelFailedEvent event, String messageId) {
        idempotencyService.idempotenceCheck(messageId, event.getClass().getSimpleName());

        log.error("An error occurred while compensating for a transaction in the UserMS for an order with the ID {}",
                event.orderId());

        orderHistoryService.createHistoryLog(event.orderId(), OrderHistoryStatus.FAIL,
                "UserMS(Warning!!!): an error occurred while compensating for a transaction; for more information, see the logs.");

        RefundPaymentCommand command = new RefundPaymentCommand(event.orderId(), event.username());
        kafkaCommandPublisher.sendRefundPaymentCommand(command.orderId().toString(), command);

        idempotencyService.saveIdempotentKey(messageId, event.getClass().getSimpleName());
    }
}
