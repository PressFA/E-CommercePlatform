package by.pressf.orderms.unit.saga.handler;

import by.pressf.core.dto.orchestration.commands.emailnotification.SendEmailOrderCommand;
import by.pressf.core.dto.orchestration.commands.order.RejectOrderCommand;
import by.pressf.core.dto.orchestration.commands.payment.RefundPaymentCommand;
import by.pressf.core.dto.orchestration.commands.product.CancelProductReservationCommand;
import by.pressf.core.dto.orchestration.events.order.OrderRejectionFailedEvent;
import by.pressf.core.dto.orchestration.events.payment.PaymentRefundFailedEvent;
import by.pressf.core.dto.orchestration.events.product.ProductReservationCancelFailedEvent;
import by.pressf.core.dto.orchestration.events.user.UserBalanceDebitCancelFailedEvent;
import by.pressf.core.exceptions.DuplicateMessageException;
import by.pressf.orderms.dao.entity.status.OrderHistoryStatus;
import by.pressf.orderms.saga.handler.CriticalAuditSagaHandler;
import by.pressf.orderms.saga.publisher.KafkaCommandPublisher;
import by.pressf.orderms.service.IdempotencyService;
import by.pressf.orderms.service.OrderHistoryService;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataAccessException;
import org.springframework.kafka.KafkaException;

import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class CriticalAuditSagaHandlerUnitTests {
    private @Mock IdempotencyService idempotencyService;
    private @Mock OrderHistoryService orderHistoryService;
    private @Mock KafkaCommandPublisher kafkaCommandPublisher;
    private @InjectMocks CriticalAuditSagaHandler handler;

    @ParameterizedTest @MethodSource("orderRejectionFailedArgs")
    void handleOrderRejectionFailed_Success_CompletesSuccessfully(OrderRejectionFailedEvent event,
                                                                  String messageId) {
        // Arrange
        doNothing().when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());
        doNothing().when(orderHistoryService)
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        doNothing().when(kafkaCommandPublisher)
                .sendSendEmailOrderCommand(anyString(), any(SendEmailOrderCommand.class));
        doNothing().when(idempotencyService)
                .saveIdempotentKey(anyString(), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> handler.handleOrderRejectionFailedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, times(1))
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, times(1))
                .sendSendEmailOrderCommand(anyString(), any(SendEmailOrderCommand.class));
        verify(idempotencyService, times(1))
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("orderRejectionFailedArgs")
    void handleOrderRejectionFailed_Duplicate_ThrowsDuplicateMessageException(OrderRejectionFailedEvent event,
                                                                              String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());

        // Act & Assert
        assertThrows(DuplicateMessageException.class,
                () -> handler.handleOrderRejectionFailedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, never())
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, never())
                .sendSendEmailOrderCommand(anyString(), any(SendEmailOrderCommand.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("orderRejectionFailedArgs")
    void handleOrderRejectionFailed_KafkaError_ThrowsKafkaException(OrderRejectionFailedEvent event,
                                                                    String messageId) {
        // Arrange
        doNothing().when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());
        doNothing().when(orderHistoryService)
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        doThrow(mock(KafkaException.class)).when(kafkaCommandPublisher)
                .sendSendEmailOrderCommand(anyString(), any(SendEmailOrderCommand.class));

        // Act & Assert
        assertThrows(KafkaException.class, () -> handler.handleOrderRejectionFailedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, times(1))
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, times(1))
                .sendSendEmailOrderCommand(anyString(), any(SendEmailOrderCommand.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("orderRejectionFailedArgs")
    void handleOrderRejectionFailed_DataAccessError_ThrowsDataAccessException(OrderRejectionFailedEvent event,
                                                                              String messageId) {
        // Arrange
        doNothing().when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());
        doNothing().when(orderHistoryService)
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        doNothing().when(kafkaCommandPublisher)
                .sendSendEmailOrderCommand(anyString(), any(SendEmailOrderCommand.class));
        doThrow(mock(DataAccessException.class)).when(idempotencyService)
                .saveIdempotentKey(anyString(), anyString());

        // Act & Assert
        assertThrows(DataAccessException.class,
                () -> handler.handleOrderRejectionFailedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, times(1))
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, times(1))
                .sendSendEmailOrderCommand(anyString(), any(SendEmailOrderCommand.class));
        verify(idempotencyService, times(1))
                .saveIdempotentKey(anyString(), anyString());
    }

    private static Stream<Arguments> orderRejectionFailedArgs() {
        return Stream.of(
                Arguments.of(new OrderRejectionFailedEvent(UUID.randomUUID(), "auditUser"),
                        "msg-audit-201")
        );
    }

    @ParameterizedTest @MethodSource("productReservationCancelFailedArgs")
    void handleProductReservationCancelFailed_Success_CompletesSuccessfully(ProductReservationCancelFailedEvent event,
                                                                            String messageId) {
        // Arrange
        doNothing().when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());
        doNothing().when(orderHistoryService)
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        doNothing().when(kafkaCommandPublisher)
                .sendRejectOrderCommand(anyString(), any(RejectOrderCommand.class));
        doNothing().when(idempotencyService)
                .saveIdempotentKey(anyString(), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> handler.handleProductReservationCancelFailedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, times(1))
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, times(1))
                .sendRejectOrderCommand(anyString(), any(RejectOrderCommand.class));
        verify(idempotencyService, times(1))
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("productReservationCancelFailedArgs")
    void handleProductReservationCancelFailed_Duplicate_ThrowsDuplicateMessageException(ProductReservationCancelFailedEvent event,
                                                                                        String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());

        // Act & Assert
        assertThrows(DuplicateMessageException.class,
                () -> handler.handleProductReservationCancelFailedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, never())
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, never())
                .sendRejectOrderCommand(anyString(), any(RejectOrderCommand.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("productReservationCancelFailedArgs")
    void handleProductReservationCancelFailed_KafkaError_ThrowsKafkaException(ProductReservationCancelFailedEvent event,
                                                                              String messageId) {
        // Arrange
        doNothing().when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());
        doNothing().when(orderHistoryService)
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        doThrow(mock(KafkaException.class)).when(kafkaCommandPublisher)
                .sendRejectOrderCommand(anyString(), any(RejectOrderCommand.class));

        // Act & Assert
        assertThrows(KafkaException.class,
                () -> handler.handleProductReservationCancelFailedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, times(1))
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, times(1))
                .sendRejectOrderCommand(anyString(), any(RejectOrderCommand.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("productReservationCancelFailedArgs")
    void handleProductReservationCancelFailed_DataAccessError_ThrowsDataAccessException(ProductReservationCancelFailedEvent event,
                                                                                        String messageId) {
        // Arrange
        doNothing().when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());
        doNothing().when(orderHistoryService)
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        doNothing().when(kafkaCommandPublisher)
                .sendRejectOrderCommand(anyString(), any(RejectOrderCommand.class));
        doThrow(mock(DataAccessException.class)).when(idempotencyService)
                .saveIdempotentKey(anyString(), anyString());

        // Act & Assert
        assertThrows(DataAccessException.class,
                () -> handler.handleProductReservationCancelFailedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, times(1))
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, times(1))
                .sendRejectOrderCommand(anyString(), any(RejectOrderCommand.class));
        verify(idempotencyService, times(1))
                .saveIdempotentKey(anyString(), anyString());
    }

    private static Stream<Arguments> productReservationCancelFailedArgs() {
        return Stream.of(
                Arguments.of(new ProductReservationCancelFailedEvent(UUID.randomUUID(),
                        "auditUser"), "msg-audit-202")
        );
    }

    @ParameterizedTest @MethodSource("paymentRefundFailedArgs")
    void handlePaymentRefundFailed_Success_CompletesSuccessfully(PaymentRefundFailedEvent event,
                                                                 String messageId) {
        // Arrange
        doNothing().when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());
        doNothing().when(orderHistoryService)
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        doNothing().when(kafkaCommandPublisher)
                .sendCancelProductReservationCommand(anyString(), any(CancelProductReservationCommand.class));
        doNothing().when(idempotencyService)
                .saveIdempotentKey(anyString(), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> handler.handlePaymentRefundFailedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, times(1))
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, times(1))
                .sendCancelProductReservationCommand(anyString(), any(CancelProductReservationCommand.class));
        verify(idempotencyService, times(1))
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("paymentRefundFailedArgs")
    void handlePaymentRefundFailed_Duplicate_ThrowsDuplicateMessageException(PaymentRefundFailedEvent event,
                                                                             String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());

        // Act & Assert
        assertThrows(DuplicateMessageException.class,
                () -> handler.handlePaymentRefundFailedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, never())
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, never())
                .sendCancelProductReservationCommand(anyString(), any(CancelProductReservationCommand.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("paymentRefundFailedArgs")
    void handlePaymentRefundFailed_KafkaError_ThrowsKafkaException(PaymentRefundFailedEvent event,
                                                                   String messageId) {
        // Arrange
        doNothing().when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());
        doNothing().when(orderHistoryService)
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        doThrow(mock(KafkaException.class)).when(kafkaCommandPublisher)
                .sendCancelProductReservationCommand(anyString(), any(CancelProductReservationCommand.class));

        // Act & Assert
        assertThrows(KafkaException.class,
                () -> handler.handlePaymentRefundFailedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, times(1))
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, times(1))
                .sendCancelProductReservationCommand(anyString(), any(CancelProductReservationCommand.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("paymentRefundFailedArgs")
    void handlePaymentRefundFailed_DataAccessError_ThrowsDataAccessException(PaymentRefundFailedEvent event,
                                                                             String messageId) {
        // Arrange
        doNothing().when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());
        doNothing().when(orderHistoryService)
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        doNothing().when(kafkaCommandPublisher)
                .sendCancelProductReservationCommand(anyString(), any(CancelProductReservationCommand.class));
        doThrow(mock(DataAccessException.class)).when(idempotencyService)
                .saveIdempotentKey(anyString(), anyString());

        // Act & Assert
        assertThrows(DataAccessException.class,
                () -> handler.handlePaymentRefundFailedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, times(1))
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, times(1))
                .sendCancelProductReservationCommand(anyString(), any(CancelProductReservationCommand.class));
        verify(idempotencyService, times(1))
                .saveIdempotentKey(anyString(), anyString());
    }

    private static Stream<Arguments> paymentRefundFailedArgs() {
        return Stream.of(
                Arguments.of(new PaymentRefundFailedEvent(UUID.randomUUID(), "auditUser"),
                        "msg-audit-203")
        );
    }

    @ParameterizedTest @MethodSource("userBalanceDebitCancelFailedArgs")
    void handleUserBalanceDebitCancelFailed_Success_CompletesSuccessfully(UserBalanceDebitCancelFailedEvent event,
                                                                          String messageId) {
        // Arrange
        doNothing().when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());
        doNothing().when(orderHistoryService)
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        doNothing().when(kafkaCommandPublisher)
                .sendRefundPaymentCommand(anyString(), any(RefundPaymentCommand.class));
        doNothing().when(idempotencyService)
                .saveIdempotentKey(anyString(), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> handler.handleUserBalanceDebitCancelFailedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, times(1))
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, times(1))
                .sendRefundPaymentCommand(anyString(), any(RefundPaymentCommand.class));
        verify(idempotencyService, times(1))
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("userBalanceDebitCancelFailedArgs")
    void handleUserBalanceDebitCancelFailed_Duplicate_ThrowsDuplicateMessageException(UserBalanceDebitCancelFailedEvent event,
                                                                                      String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());

        // Act & Assert
        assertThrows(DuplicateMessageException.class,
                () -> handler.handleUserBalanceDebitCancelFailedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, never())
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, never())
                .sendRefundPaymentCommand(anyString(), any(RefundPaymentCommand.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("userBalanceDebitCancelFailedArgs")
    void handleUserBalanceDebitCancelFailed_KafkaError_ThrowsKafkaException(UserBalanceDebitCancelFailedEvent event,
                                                                            String messageId) {
        // Arrange
        doNothing().when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());
        doNothing().when(orderHistoryService)
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        doThrow(mock(KafkaException.class)).when(kafkaCommandPublisher)
                .sendRefundPaymentCommand(anyString(), any(RefundPaymentCommand.class));

        // Act & Assert
        assertThrows(KafkaException.class,
                () -> handler.handleUserBalanceDebitCancelFailedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, times(1))
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, times(1))
                .sendRefundPaymentCommand(anyString(), any(RefundPaymentCommand.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("userBalanceDebitCancelFailedArgs")
    void handleUserBalanceDebitCancelFailed_DataAccessError_ThrowsDataAccessException(UserBalanceDebitCancelFailedEvent event,
                                                                                      String messageId) {
        // Arrange
        doNothing().when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());
        doNothing().when(orderHistoryService)
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        doNothing().when(kafkaCommandPublisher)
                .sendRefundPaymentCommand(anyString(), any(RefundPaymentCommand.class));
        doThrow(mock(DataAccessException.class)).when(idempotencyService)
                .saveIdempotentKey(anyString(), anyString());

        // Act & Assert
        assertThrows(DataAccessException.class,
                () -> handler.handleUserBalanceDebitCancelFailedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, times(1))
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, times(1))
                .sendRefundPaymentCommand(anyString(), any(RefundPaymentCommand.class));
        verify(idempotencyService, times(1))
                .saveIdempotentKey(anyString(), anyString());
    }

    private static Stream<Arguments> userBalanceDebitCancelFailedArgs() {
        return Stream.of(
                Arguments.of(new UserBalanceDebitCancelFailedEvent(UUID.randomUUID(),
                        "auditUser"), "msg-audit-204")
        );
    }
}
