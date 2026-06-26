package by.pressf.orderms.unit.saga.handler;

import by.pressf.core.dto.orchestration.commands.order.RejectOrderCommand;
import by.pressf.core.dto.orchestration.commands.payment.RefundPaymentCommand;
import by.pressf.core.dto.orchestration.commands.product.CancelProductReservationCommand;
import by.pressf.core.dto.orchestration.commands.user.CancelUserBalanceDebitCommand;
import by.pressf.core.dto.orchestration.events.order.OrderCompletionFailedEvent;
import by.pressf.core.dto.orchestration.events.payment.PaymentChargeFailedEvent;
import by.pressf.core.dto.orchestration.events.product.ProductReservationFailedEvent;
import by.pressf.core.dto.orchestration.events.user.UserBalanceDebitFailedEvent;
import by.pressf.core.exceptions.DuplicateMessageException;
import by.pressf.orderms.dao.entity.status.OrderHistoryStatus;
import by.pressf.orderms.saga.handler.CompensationSagaHandler;
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

import java.math.BigDecimal;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class CompensationSagaHandlerUnitTests {
    private @Mock IdempotencyService idempotencyService;
    private @Mock OrderHistoryService orderHistoryService;
    private @Mock KafkaCommandPublisher kafkaCommandPublisher;
    private @InjectMocks CompensationSagaHandler handler;

    @ParameterizedTest @MethodSource("productReservationFailedArgs")
    void handleProductReservationFailed_Success_CompletesSuccessfully(ProductReservationFailedEvent event,
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
        assertDoesNotThrow(() -> handler.handleProductReservationFailedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, times(1))
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, times(1))
                .sendRejectOrderCommand(anyString(), any(RejectOrderCommand.class));
        verify(idempotencyService, times(1))
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("productReservationFailedArgs")
    void handleProductReservationFailed_Duplicate_ThrowsDuplicateMessageException(ProductReservationFailedEvent event,
                                                                                  String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());

        // Act & Assert
        assertThrows(DuplicateMessageException.class,
                () -> handler.handleProductReservationFailedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, never())
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, never())
                .sendRejectOrderCommand(anyString(), any(RejectOrderCommand.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("productReservationFailedArgs")
    void handleProductReservationFailed_KafkaError_ThrowsKafkaException(ProductReservationFailedEvent event,
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
                () -> handler.handleProductReservationFailedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, times(1))
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, times(1))
                .sendRejectOrderCommand(anyString(), any(RejectOrderCommand.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("productReservationFailedArgs")
    void handleProductReservationFailed_DataAccessError_ThrowsDataAccessException(ProductReservationFailedEvent event,
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
                () -> handler.handleProductReservationFailedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, times(1))
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, times(1))
                .sendRejectOrderCommand(anyString(), any(RejectOrderCommand.class));
        verify(idempotencyService, times(1))
                .saveIdempotentKey(anyString(), anyString());
    }

    private static Stream<Arguments> productReservationFailedArgs() {
        return Stream.of(
                Arguments.of(new ProductReservationFailedEvent(UUID.randomUUID(), "sagaUser"),
                        "msg-saga-101")
        );
    }

    @ParameterizedTest @MethodSource("paymentChargeFailedArgs")
    void handlePaymentChargeFailed_Success_CompletesSuccessfully(PaymentChargeFailedEvent event,
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
        assertDoesNotThrow(() -> handler.handlePaymentChargeFailedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, times(1))
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, times(1))
                .sendCancelProductReservationCommand(anyString(), any(CancelProductReservationCommand.class));
        verify(idempotencyService, times(1))
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("paymentChargeFailedArgs")
    void handlePaymentChargeFailed_Duplicate_ThrowsDuplicateMessageException(PaymentChargeFailedEvent event,
                                                                             String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());

        // Act & Assert
        assertThrows(DuplicateMessageException.class,
                () -> handler.handlePaymentChargeFailedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, never())
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, never())
                .sendCancelProductReservationCommand(anyString(), any(CancelProductReservationCommand.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("paymentChargeFailedArgs")
    void handlePaymentChargeFailed_KafkaError_ThrowsKafkaException(PaymentChargeFailedEvent event,
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
                () -> handler.handlePaymentChargeFailedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, times(1))
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, times(1))
                .sendCancelProductReservationCommand(anyString(), any(CancelProductReservationCommand.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("paymentChargeFailedArgs")
    void handlePaymentChargeFailed_DataAccessError_ThrowsDataAccessException(PaymentChargeFailedEvent event,
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
                () -> handler.handlePaymentChargeFailedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, times(1))
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, times(1))
                .sendCancelProductReservationCommand(anyString(), any(CancelProductReservationCommand.class));
        verify(idempotencyService, times(1))
                .saveIdempotentKey(anyString(), anyString());
    }

    private static Stream<Arguments> paymentChargeFailedArgs() {
        return Stream.of(
                Arguments.of(new PaymentChargeFailedEvent(UUID.randomUUID(), "sagaUser"),
                        "msg-saga-102")
        );
    }

    @ParameterizedTest @MethodSource("userBalanceDebitFailedArgs")
    void handleUserBalanceDebitFailed_Success_CompletesSuccessfully(UserBalanceDebitFailedEvent event,
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
        assertDoesNotThrow(() -> handler.handleUserBalanceDebitFailedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, times(1))
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, times(1))
                .sendRefundPaymentCommand(anyString(), any(RefundPaymentCommand.class));
        verify(idempotencyService, times(1))
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("userBalanceDebitFailedArgs")
    void handleUserBalanceDebitFailed_Duplicate_ThrowsDuplicateMessageException(UserBalanceDebitFailedEvent event,
                                                                                String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());

        // Act & Assert
        assertThrows(DuplicateMessageException.class,
                () -> handler.handleUserBalanceDebitFailedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, never())
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, never())
                .sendRefundPaymentCommand(anyString(), any(RefundPaymentCommand.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("userBalanceDebitFailedArgs")
    void handleUserBalanceDebitFailed_KafkaError_ThrowsKafkaException(UserBalanceDebitFailedEvent event,
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
                () -> handler.handleUserBalanceDebitFailedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, times(1))
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, times(1))
                .sendRefundPaymentCommand(anyString(), any(RefundPaymentCommand.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("userBalanceDebitFailedArgs")
    void handleUserBalanceDebitFailed_DataAccessError_ThrowsDataAccessException(UserBalanceDebitFailedEvent event,
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
                () -> handler.handleUserBalanceDebitFailedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, times(1))
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, times(1))
                .sendRefundPaymentCommand(anyString(), any(RefundPaymentCommand.class));
        verify(idempotencyService, times(1))
                .saveIdempotentKey(anyString(), anyString());
    }

    private static Stream<Arguments> userBalanceDebitFailedArgs() {
        return Stream.of(
                Arguments.of(new UserBalanceDebitFailedEvent(UUID.randomUUID(), "sagaUser"),
                        "msg-saga-103")
        );
    }

    @ParameterizedTest @MethodSource("orderCompletionFailedArgs")
    void handleOrderCompletionFailed_Success_CompletesSuccessfully(OrderCompletionFailedEvent event,
                                                                   String messageId) {
        // Arrange
        doNothing().when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());
        doNothing().when(orderHistoryService)
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        doNothing().when(kafkaCommandPublisher)
                .sendCancelUserBalanceDebitCommand(anyString(), any(CancelUserBalanceDebitCommand.class));
        doNothing().when(idempotencyService)
                .saveIdempotentKey(anyString(), anyString());

        // Act & Assert
        assertDoesNotThrow(() -> handler.handleOrderCompletionFailedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, times(1))
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, times(1))
                .sendCancelUserBalanceDebitCommand(anyString(), any(CancelUserBalanceDebitCommand.class));
        verify(idempotencyService, times(1))
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("orderCompletionFailedArgs")
    void handleOrderCompletionFailed_Duplicate_ThrowsDuplicateMessageException(OrderCompletionFailedEvent event,
                                                                               String messageId) {
        // Arrange
        doThrow(mock(DuplicateMessageException.class)).when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());

        // Act & Assert
        assertThrows(DuplicateMessageException.class,
                () -> handler.handleOrderCompletionFailedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, never())
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, never())
                .sendCancelUserBalanceDebitCommand(anyString(), any(CancelUserBalanceDebitCommand.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("orderCompletionFailedArgs")
    void handleOrderCompletionFailed_KafkaError_ThrowsKafkaException(OrderCompletionFailedEvent event,
                                                                     String messageId) {
        // Arrange
        doNothing().when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());
        doNothing().when(orderHistoryService)
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        doThrow(mock(KafkaException.class)).when(kafkaCommandPublisher)
                .sendCancelUserBalanceDebitCommand(anyString(), any(CancelUserBalanceDebitCommand.class));

        // Act & Assert
        assertThrows(KafkaException.class,
                () -> handler.handleOrderCompletionFailedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, times(1))
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, times(1))
                .sendCancelUserBalanceDebitCommand(anyString(), any(CancelUserBalanceDebitCommand.class));
        verify(idempotencyService, never())
                .saveIdempotentKey(anyString(), anyString());
    }

    @ParameterizedTest @MethodSource("orderCompletionFailedArgs")
    void handleOrderCompletionFailed_DataAccessError_ThrowsDataAccessException(OrderCompletionFailedEvent event,
                                                                               String messageId) {
        // Arrange
        doNothing().when(idempotencyService)
                .idempotenceCheck(anyString(), anyString());
        doNothing().when(orderHistoryService)
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        doNothing().when(kafkaCommandPublisher)
                .sendCancelUserBalanceDebitCommand(anyString(), any(CancelUserBalanceDebitCommand.class));
        doThrow(mock(DataAccessException.class)).when(idempotencyService)
                .saveIdempotentKey(anyString(), anyString());

        // Act & Assert
        assertThrows(DataAccessException.class,
                () -> handler.handleOrderCompletionFailedEvent(event, messageId));

        verify(idempotencyService, times(1))
                .idempotenceCheck(anyString(), anyString());
        verify(orderHistoryService, times(1))
                .createHistoryLog(any(UUID.class), any(OrderHistoryStatus.class), anyString());
        verify(kafkaCommandPublisher, times(1))
                .sendCancelUserBalanceDebitCommand(anyString(), any(CancelUserBalanceDebitCommand.class));
        verify(idempotencyService, times(1))
                .saveIdempotentKey(anyString(), anyString());
    }

    private static Stream<Arguments> orderCompletionFailedArgs() {
        return Stream.of(
                Arguments.of(new OrderCompletionFailedEvent(UUID.randomUUID(), UUID.randomUUID(),
                        "sagaUser", BigDecimal.TEN), "msg-saga-104")
        );
    }
}
